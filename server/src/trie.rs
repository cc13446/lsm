use tokio::fs::File;
use std::mem::MaybeUninit;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use crate::event::LEN_MASK;

const NODE_SIZE: usize = 1 << 8;

pub struct Trie {
    nodes: Box<[Option<Trie>; NODE_SIZE]>,
    value: Option<Vec<u8>>,
}

impl Clone for Trie {
    fn clone(&self) -> Self {
        let mut res = Trie::new();
        res.clone_from(self);
        res
    }

    fn clone_from(&mut self, source: &Self) {
        for i in 0..NODE_SIZE {
            self.nodes[i] = source.nodes[i].clone();
        }
        self.value = source.value.clone();
    }
}

impl Trie {
    pub fn new() -> Self {
        let mut node: [MaybeUninit<Option<Trie>>; NODE_SIZE] = unsafe { MaybeUninit::uninit().assume_init() };
        for i in 0..NODE_SIZE {
            node[i] = MaybeUninit::new(None);
        }

        Trie {
            nodes: Box::new(unsafe { core::mem::transmute(node) }),
            value: None,
        }
    }

    pub fn set(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) {
        self.do_set(key, value, 0)
    }

    fn do_set(&mut self, key: Vec<u8>, value: Option<Vec<u8>>, index: usize) {
        if index > key.len() {
            return;
        } else if index == key.len() {
            self.value = value;
        } else {
            let i = key[index] as usize;
            match self.nodes[i].as_mut() {
                Some(node) => {
                    node.do_set(key, value, index + 1)
                }
                None => {
                    let mut node = Trie::new();
                    node.do_set(key, value, index + 1);
                    self.nodes[i] = Some(node);
                }
            }
        }
    }

    pub fn get(&self, key: Vec<u8>) -> Option<Vec<u8>> {
        self.do_get(key, 0)
    }

    fn do_get(&self, key: Vec<u8>, index: usize) -> Option<Vec<u8>> {
        if index > key.len() {
            None
        } else if index == key.len() {
            self.value.clone()
        } else {
            let i = key[index] as usize;
            match self.nodes[i].as_ref() {
                Some(node) => {
                    node.do_get(key, index + 1)
                }
                None => None
            }
        }
    }

    pub fn save(&self, file: &Arc<Mutex<File>>) {
        let mut key = Vec::new();
        self.do_save(file, &mut key)
    }

    fn do_save(&self, file: &Arc<Mutex<File>>, key: &mut Vec<u8>) {
        async fn do_write(file: &Arc<Mutex<File>>, key: &Vec<u8>, value: &Vec<u8>) {
            let err_message = "Write log file fail";
            file.lock().await.write_u8(((key.len() as u16 & LEN_MASK) >> 8) as u8).await.expect(err_message);
            file.lock().await.write_u8(key.len() as u8).await.expect(err_message);
            file.lock().await.write_all(key.as_slice()).await.expect(err_message);
            file.lock().await.write_u8(((value.len() as u16 & LEN_MASK) >> 8) as u8).await.expect(err_message);
            file.lock().await.write_u8(value.len() as u8).await.expect(err_message);
            file.lock().await.write_all(value.as_slice()).await.expect(err_message);
        }
        futures::executor::block_on(async {
            if self.value.is_some() {
                do_write(file, key, self.value.as_ref().unwrap()).await;
            }
        });
        for i in 0..NODE_SIZE {
            if let Some(n) = self.nodes[i].as_ref() {
                key.push(i as u8);
                n.do_save(file, key);
                key.pop();
            }
        }
    }
}