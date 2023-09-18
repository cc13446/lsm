use std::fs::create_dir_all;
use std::sync::Arc;
use dashmap::DashMap;
use log::{info, warn};
use tokio::fs::{File, try_exists};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc::Receiver;
use crate::client::Client;
use crate::trie::Trie;

pub const OP_GET: u8 = 0xc1;
pub const OP_SET: u8 = 0xc2;

pub const RES_GET: u8 = 0x81;
pub const RES_SET: u8 = 0x82;

pub const LEN_MASK: u16 = 0x7fff;
pub const NONE_VALUE_LEN: u16 = 0xffff;

const WAL_FILE_PREFIX: &str = "WAL_FILE_";
const LOG_FILE_PREFIX: &str = "LOG_FILE_";
const INDEX_FILE: &str = "INDEX";

const FILE_BATCH: usize = 2;

pub enum Event {
    GET {
        id: String,
        key: Vec<u8>,
    },
    SET {
        id: String,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
    },
}

#[derive(Debug)]
pub enum EventRes {
    GET {
        id: String,
        value: Option<Vec<u8>>,
    },
    SET {
        id: String,
    },
}

pub struct EventHandler {
    receiver: Receiver<Event>,
    trie: Trie,
    client_map: Arc<DashMap<String, Client>>,
    wal_files: Vec<File>,
    log_files: Vec<File>,
    index_file: File,
    file_index: usize,
}

impl EventHandler {
    pub async fn new(receiver: Receiver<Event>, trie: Trie, client_map: Arc<DashMap<String, Client>>, data_path: String) -> Self {
        // dir
        if !try_exists(&data_path).await.unwrap_or_else(|e| { panic!("Try exists fail, err = {:?}", e); }) {
            create_dir_all(&data_path).unwrap_or_else(|e| { panic!("Create data dir fail, err = {:?}", e) });
        }
        // file
        let mut wal_files = Vec::new();
        let mut log_files = Vec::new();

        async fn open_file(file_name: String, append: bool) -> File {
            File::options().append(append).read(true).write(true).create(true).open(file_name).await.unwrap_or_else(|e| {
                panic!("Open file err {:?}", e);
            })
        }
        let index_file_name = format!("{}/{}", &data_path, INDEX_FILE);
        info!("LSM open index file {}", &index_file_name);
        let mut index_file = open_file(index_file_name, false).await;
        let file_index = match index_file.read_u8().await {
            Ok(n) => n as usize,
            Err(e) => {
                info!("Read index file err {:?}", e);
                let i = 0;
                index_file.write_u8(i as u8).await.unwrap_or_else(|e| { panic!("Write index file fail err = {:?}", e) });
                index_file.flush().await.unwrap_or_else(|e| { panic!("Flush index file fail err = {:?}", e) });
                i
            }
        };
        info!("File index is {}", file_index);

        for i in 0..FILE_BATCH {
            let wal_file_name = format!("{}/{}{}", &data_path, WAL_FILE_PREFIX, i);
            let log_file_name = format!("{}/{}{}", &data_path, LOG_FILE_PREFIX, i);
            info!("LSM open file {}", &wal_file_name);
            let wal_file = open_file(wal_file_name, true).await;
            info!("LSM open file {}", &log_file_name);
            let log_file = open_file(log_file_name, true).await;

            wal_files.push(wal_file);
            log_files.push(log_file);
        }
        Self {
            receiver,
            trie,
            client_map,
            wal_files,
            log_files,
            index_file,
            file_index,
        }
    }

    pub async fn start_event_loop(&mut self) {
        // read from Log file

        // read from WAL file

        // do
        info!("LSM server start event loop");
        loop {
            match self.receiver.recv().await {
                Some(event) => {
                    match event {
                        Event::GET { id, key } => {
                            info!("Receive get event, id = {}, key = {:?}", &id, &key);
                            let client_option = self.client_map.get_mut(&id);
                            match client_option {
                                None => {
                                    info!("Don't have client id = {}", &id)
                                }
                                Some(mut client_entry) => {
                                    client_entry.value_mut().send_event_res(EventRes::GET {
                                        id: id.clone(),
                                        value: self.trie.get(key),
                                    }).await;
                                }
                            }
                        }
                        Event::SET { id, key, value } => {
                            // WAL

                            // do set
                            info!("Receive set event, id = {}, key = {:?}, value = {:?}", &id, &key, &value);
                            let client_option = self.client_map.get_mut(&id);
                            match client_option {
                                None => {
                                    info!("Don't have client id = {}", &id)
                                }
                                Some(mut client_entry) => {
                                    self.trie.set(key, value);
                                    client_entry.value_mut().send_event_res(EventRes::SET {
                                        id: id.clone(),
                                    }).await;
                                }
                            }
                        }
                    }
                }
                None => {
                    warn!("Receive event none");
                }
            }
        }
    }
}
