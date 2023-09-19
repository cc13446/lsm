use std::fs::create_dir_all;
use std::io::SeekFrom;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use dashmap::DashMap;
use log::{info, warn};
use tokio::fs::{File, try_exists};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc::Receiver;
use tokio::sync::Mutex;
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
    log_files: Vec<Arc<Mutex<File>>>,
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
        let index_file = open_file(index_file_name, false).await;
        for i in 0..FILE_BATCH {
            let wal_file_name = format!("{}/{}{}", &data_path, WAL_FILE_PREFIX, i);
            let log_file_name = format!("{}/{}{}", &data_path, LOG_FILE_PREFIX, i);
            info!("LSM open file {}", &wal_file_name);
            let wal_file = open_file(wal_file_name, true).await;
            info!("LSM open file {}", &log_file_name);
            let log_file = open_file(log_file_name, true).await;

            wal_files.push(wal_file);
            log_files.push(Arc::new(Mutex::new(log_file)));
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
        self.index_file.seek(SeekFrom::Start(0)).await.expect("Seek index file fail ");
        self.index_file.write_u8(index).await.expect("Write index file fail err");
        self.index_file.sync_all().await.expect("Flush index file fail");
    }

    async fn load(&mut self, buf: Vec<u8>) {
        // 2 bit key length
        // n bit key
        // 2 bit value length
        // n bit value
        let mut index = 0;
        let len = buf.len();
        while index < len {
            if index + 2 > len {
                break;
            }
            let key_len = (buf[index] as usize * 0x100 + buf[index + 1] as usize) & LEN_MASK as usize;
            if index + 2 + key_len + 2 > len {
                break;
            }
            let key = Vec::from(&buf[index + 2..index + 2 + key_len]);
            let value_len = buf[index + 2 + key_len] as usize * 0x100 + buf[index + 2 + key_len + 1] as usize;
            if value_len == NONE_VALUE_LEN as usize {
                self.trie.set(key, None);
                index += 2 + key_len + 2;
            } else {
                let value_len = value_len & LEN_MASK as usize;
                if index + 2 + key_len + 2 + value_len > len {
                    break;
                }
                let value = Vec::from(&buf[index + 2 + key_len + 2..index + 2 + key_len + 2 + value_len]);
                self.trie.set(key, Some(value));
                index += 2 + key_len + 2 + value_len;
            }
        }
    }

    pub async fn start_event_loop(&mut self) {
        // read index
        let mut file_index = match self.index_file.read_u8().await {
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

        // read from LOG file and WAL file
        let file_index_last = FILE_BATCH - 1 - file_index;
        // last log
        let mut log_file_last_content = Vec::new();
        self.log_files[file_index_last].lock().await.read_to_end(&mut log_file_last_content).await.expect("Read last log file fail");
        self.load(log_file_last_content).await;
        // last wal
        let mut wal_file_last_content = Vec::new();
        self.wal_files[file_index_last].read_to_end(&mut wal_file_last_content).await.expect("Read last wal file fail");
        self.load(wal_file_last_content).await;
        // this log
        let mut log_file_this_content = Vec::new();
        self.log_files[file_index].lock().await.read_to_end(&mut log_file_this_content).await.expect("Read this log file fail");
        self.load(log_file_this_content).await;
        // this wal
        let mut wal_file_this_content = Vec::new();
        self.wal_files[file_index].read_to_end(&mut wal_file_this_content).await.expect("Read this wal file fail");
        self.load(wal_file_this_content).await;

        // is saving
        let saving = Arc::new(AtomicBool::new(false));

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
                            let mut buf = Vec::new();
                            buf.push(((key.len() as u16 & LEN_MASK) >> 8) as u8);
                            buf.push(key.len() as u8);
                            buf.extend(key.iter());
                            match &value {
                                None => {
                                    buf.push((NONE_VALUE_LEN >> 8) as u8);
                                    buf.push(NONE_VALUE_LEN as u8);
                                }
                                Some(v) => {
                                    buf.push(((v.len() as u16 & LEN_MASK) >> 8) as u8);
                                    buf.push(v.len() as u8);
                                    buf.extend(v.iter());
                                }
                            }
                            self.wal_files[file_index].write_all(&buf).await.expect("Write wal file fail");

                            // do set
                            info!("Receive set event, id = {}, key = {:?}, value = {:?}", &id, &key, &value);
                            match self.client_map.get_mut(&id) {
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
            // check wal file size:10M
            if self.wal_files[file_index].metadata().await.expect("Read wal file meta fail").len() > 1 * 1024 * 1024 * 10 && !saving.load(Ordering::Relaxed) {
                // change file index
                file_index = FILE_BATCH - 1 - file_index;
                self.refresh_index_file(file_index as u8).await;
                // clear wal file
                self.wal_files[file_index].set_len(0).await.expect("Set this wal len zero err");

                // save the log file
                let clone_trie = self.trie.clone();
                let file = self.log_files[file_index].clone();
                let clone_saving = saving.clone();
                tokio::spawn(async move {
                    info!("Save to log file");
                    clone_saving.store(true, Ordering::Relaxed);
                    file.lock().await.set_len(0).await.expect("Set this log file len zero err");
                    clone_trie.save(&file);
                    clone_saving.store(false, Ordering::Relaxed);
                    info!("Save to log file done");
                });
            }
        }
    }
}
