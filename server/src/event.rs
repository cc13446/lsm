use std::fs::create_dir_all;
use std::io::SeekFrom;
use std::sync::Arc;
use dashmap::DashMap;
use log::{info, warn};
use tokio::fs::{File, try_exists};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
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
        }
    }

    async fn refresh_index_file(&mut self, index: u8) {
        self.index_file.seek(SeekFrom::Start(0)).await.unwrap_or_else(|e| { panic!("Seek index file fail err = {:?}", e) });
        self.index_file.write_u8(index).await.unwrap_or_else(|e| { panic!("Write index file fail err = {:?}", e) });
        self.index_file.sync_all().await.unwrap_or_else(|e| { panic!("Flush index file fail err = {:?}", e) });
    }

    pub async fn start_event_loop(&mut self) {
        // read index
        let file_index = match self.index_file.read_u8().await {
            Ok(n) if n == 1 || n == 0 => n,
            Ok(n) => {
                info!("Read index invalid {} default 0", n);
                let i = 0;
                self.refresh_index_file(i).await;
                i
            }
            Err(e) => {
                info!("Read index file err {:?}", e);
                let i = 0;
                self.refresh_index_file(i).await;
                i
            }
        } as usize;

        info!("File index is {}", file_index);
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
