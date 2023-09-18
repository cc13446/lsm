use std::mem::MaybeUninit;

const NODE_SIZE: usize = 1 << 8;

pub struct Trie {
    nodes: Box<[Option<Trie>; NODE_SIZE]>,
    value: Option<Vec<u8>>,
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
}