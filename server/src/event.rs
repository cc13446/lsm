use std::sync::Arc;
use dashmap::DashMap;
use log::{info, warn};
use tokio::sync::mpsc::Receiver;
use crate::client::Client;
use crate::trie::Trie;

pub const OP_GET: u8 = 0xc1;
pub const OP_SET: u8 = 0xc2;

pub const RES_GET: u8 = 0x81;
pub const RES_SET: u8 = 0x82;

pub const LEN_MASK: u16 = 0x7fff;

pub enum Event {
    GET {
        id: String,
        key: Vec<u8>,
    },
    SET {
        id: String,
        key: Vec<u8>,
        value: Vec<u8>,
    },
}

#[derive(Debug)]
pub enum EventRes {
    GET {
        id: String,
        value: Vec<u8>,
    },
    SET {
        id: String,
    },
}

pub struct EventHandler {
    receiver: Receiver<Event>,
    trie: Trie,
    client_map: Arc<DashMap<String, Client>>,
}

impl EventHandler {
    pub fn new(receiver: Receiver<Event>, trie: Trie, client_map: Arc<DashMap<String, Client>>) -> Self {
        Self {
            receiver,
            trie,
            client_map,
        }
    }

    pub async fn start_event_loop(&mut self) {
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
                                    // todo get
                                    client_entry.value_mut().send_event_res(EventRes::GET {
                                        id: id.clone(),
                                        value: vec![101, 101],
                                    }).await;
                                }
                            }
                        }
                        Event::SET { id, key, value } => {
                            info!("Receive set event, id = {}, key = {:?}, value = {:?}", &id, &key, &value);
                            let client_option = self.client_map.get_mut(&id);
                            match client_option {
                                None => {
                                    info!("Don't have client id = {}", &id)
                                }
                                Some(mut client_entry) => {
                                    // todo set
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
