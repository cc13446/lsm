mod event;
mod client;
mod utils;
mod trie;

use std::collections::HashMap;
use log::{error, info, warn};
use std::env;
use std::sync::Arc;
use dashmap::DashMap;
use serde_derive::Deserialize;
use tokio::fs::File;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::select;
use tokio::sync::mpsc;
use crate::client::Client;
use crate::event::{Event, EventHandler, EventRes, LEN_MASK, RES_GET, RES_SET};
use crate::trie::Trie;
use crate::utils::get_id;

const SUB: &str = "-";

// 握手数字
const HELLO_NUM: u8 = 77;

// 文件配置参数
#[derive(Deserialize)]
struct FileConfig {
    ip: String,
    port: u32,
}

// 命令行参数
struct EnvConfig {
    config_file_path: String,
}

fn get_config_file_path(args_map: &HashMap<String, String>, default: &String) -> String {
    String::from(args_map.get("-f").unwrap_or(args_map.get("--config-file").unwrap_or(default)))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // init logger
    env_logger::init();

    // parse env config
    let args: Vec<String> = env::args().collect();
    let mut args_map: HashMap<String, String> = HashMap::new();

    for i in 0..args.len() {
        if args[i] == (String::from(SUB) + SUB) {
            break;
        }
        if args[i].starts_with(SUB) && i < args.len() - 1 && !args[i + 1].starts_with(SUB) {
            args_map.insert(args[i].clone(), args[i + 1].clone());
        }
    }

    let env_config = EnvConfig {
        config_file_path: String::from(get_config_file_path(&args_map, &String::from("./server_config.toml"))),
    };

    info!("LSM server start with config");
    info!("LSM server config file path {}", &env_config.config_file_path);

    // parse file config
    let mut file = match File::open(&env_config.config_file_path).await {
        Ok(f) => f,
        Err(e) => panic!("Error when read config file : {} err : {}", &env_config.config_file_path, e)
    };

    let mut config_str = String::new();
    match file.read_to_string(&mut config_str).await {
        Ok(s) => s,
        Err(e) => panic!("Error when reading file : {} err : {}", &env_config.config_file_path, e)
    };

    info!("LSM server file config \n{}", &config_str);

    let file_config: FileConfig = match toml::from_str(&config_str) {
        Ok(s) => s,
        Err(e) => panic!("Error when convert str to config err : {}", e),
    };

    info!("LSM server start with ip {} port {}", file_config.ip, file_config.port);

    // create event mpsc
    let (event_tx, event_rx) = mpsc::channel(128);
    info!("LSM server create event mpsc");

    // clientMap
    let client_map: Arc<DashMap<String, Client>> = Arc::new(DashMap::new());
    let event_client_map = client_map.clone();

    // create event loop
    tokio::spawn(async move {
        let trie = Trie::new();
        let mut event_handler = EventHandler::new(event_rx, trie, event_client_map);
        event_handler.start_event_loop().await;
        panic!("Event loop end!!!")
    });

    info!("LSM server create event loop");

    // create tcp
    let addr = format!("{}:{}", &file_config.ip, &file_config.port);
    let listener = TcpListener::bind(&addr).await.unwrap_or_else(|err| {
        panic!("Fail to open tcp server; err = {:?}", err);
    });
    info!("LSM server bind socket");

    // tcp close func
    async fn shutdown(id: &String, client_map: &Arc<DashMap<String, Client>>, mut socket: TcpStream) {
        info!("Client [{}] disconnect", id);
        client_map.remove(id);
        socket.shutdown().await.unwrap_or_else(|e| {
            info!("Fail close client [{}]; err = {:?} ", id, e);
        });
    }

    loop {
        match listener.accept().await {
            // new client
            Ok((mut socket, addr)) => {
                let event_tx = event_tx.clone();
                let client_map_clone = client_map.clone();
                tokio::spawn(async move {
                    // create client mpsc
                    let (client_tx, mut client_rx) = mpsc::channel(16);
                    let id = get_id(&addr.ip().to_string(), addr.port());
                    info!("Receive connection from [{}]", id);
                    // create client
                    let client = Client::new(id.clone(), client_tx);
                    client_map_clone.insert(id.clone(), client);
                    info!("New client from id [{}]", id);

                    // write hello
                    info!("Hello to client {}", id);
                    if let Err(e) = socket.write_u8(HELLO_NUM).await {
                        eprintln!("Failed to write hello to [{}]; err = {:?}", id, e);
                        shutdown(&id, &client_map_clone, socket).await;
                        return;
                    }

                    // wait hello
                    match socket.read_u8().await {
                        Ok(n) if n != HELLO_NUM => {
                            warn!("Client [{}] verify hello fail", id);
                            shutdown(&id, &client_map_clone, socket).await;
                            return;
                        }
                        Ok(n) => n,
                        Err(e) => {
                            eprintln!("Failed to read hello from [{}]; err = {:?}", id, e);
                            shutdown(&id, &client_map_clone, socket).await;
                            return;
                        }
                    };

                    // 消息缓存
                    let mut b = Vec::new();
                    let mut buf = [0; 1024];
                    info!("Alloc buffer for client [{}]", id);

                    loop {
                        // 解析消息
                        if let Some(op) = b.first() {
                            match *op {
                                event::OP_GET => {
                                    if b.len() > 3 {
                                        let key_len = ((b[1] as usize) * 0x100 + b[2] as usize) & event::LEN_MASK as usize;
                                        // 1 bit op
                                        // 2 bit key len
                                        // n bit key
                                        if b.len() >= (1 + 2 + key_len) {
                                            let next = b.split_off(1 + 2 + key_len);
                                            let content = b.split_off(1 + 2);
                                            b = next;
                                            info!("Receive get from [{}] len {} content {:?}", id, &key_len, &content);
                                            let event = Event::GET {
                                                id: id.clone(),
                                                key: content,
                                            };
                                            event_tx.send(event).await.unwrap_or_else(|e| {
                                                error!("Client {} send event error; {:?}", id, e);
                                            });
                                        }
                                    }
                                }
                                event::OP_SET => {
                                    if b.len() > 3 {
                                        let key_len = ((b[1] as usize) * 0x100 + b[2] as usize) & event::LEN_MASK as usize;
                                        // 1 bit op
                                        // 2 bit key len
                                        // n bit key
                                        // 2 bit value len
                                        // n bit value
                                        if b.len() >= (1 + 2 + key_len + 2) {
                                            let value_len = ((b[1 + 2 + key_len] as usize) * 0x100 + b[1 + 2 + key_len + 1] as usize) & event::LEN_MASK as usize;
                                            if b.len() >= (1 + 2 + key_len + 2 + value_len) {
                                                let next = b.split_off(1 + 2 + key_len + 2 + value_len);
                                                let mut pre_value = b.split_off(1 + 2 + key_len);
                                                let mut pre_key = b;
                                                let value = pre_value.split_off(2);
                                                let key = pre_key.split_off(1 + 2);
                                                b = next;
                                                info!("Receive set from [{}] len {} key {:?} value {:?}", id, &key_len, &key, &value);
                                                let event = Event::SET {
                                                    id: id.clone(),
                                                    key,
                                                    value,
                                                };
                                                event_tx.send(event).await.unwrap_or_else(|e| {
                                                    error!("Client {} send event error; {:?}", id, e);
                                                });
                                            }
                                        }
                                    }
                                }
                                n => {
                                    warn!("Unknown op {} from client [{}]", n, id);
                                    shutdown(&id, &client_map_clone, socket).await;
                                    return;
                                }
                            }
                        }
                        // 读取消息
                        select! {
                            read_res = socket.read(&mut buf) => {
                                match read_res {
                                    Ok(n) => {
                                        if n == 0 {
                                            warn!("Client [{}] read fail", id);
                                            shutdown(&id, &client_map_clone, socket).await;
                                            return;
                                        } else {
                                            b.extend_from_slice(&buf[0..n]);
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!("Failed to read from [{}]; err = {:?}", id, e);
                                        return;
                                    }
                                }
                            }
                            message = client_rx.recv() => {
                                match message {
                                    Some(event_res) => {
                                        match event_res {
                                            EventRes::GET {id, value} => {
                                                info!("Receive get event result, value = {:?}", &value);
                                                if let Err(e) = socket.write_u8(RES_GET).await {
                                                    eprintln!("Failed to write get result op to [{}]; err = {:?}", id, e);
                                                    shutdown(&id, &client_map_clone, socket).await;
                                                    return;
                                                };
                                                let len = value.len() as u16 & LEN_MASK;
                                                if let Err(e) = socket.write_u16(len).await {
                                                    eprintln!("Failed to write get result len to [{}]; err = {:?}", id, e);
                                                    shutdown(&id, &client_map_clone, socket).await;
                                                    return;
                                                };
                                                if let Err(e) = socket.write_all(value.as_slice()).await {
                                                    eprintln!("Failed to write get result len to [{}]; err = {:?}", id, e);
                                                    shutdown(&id, &client_map_clone, socket).await;
                                                    return;
                                                };
                                            },
                                            EventRes::SET {id} => {
                                                info!("Receive set event result");
                                                if let Err(e) = socket.write_u8(RES_SET).await {
                                                    eprintln!("Failed to write get result op to [{}]; err = {:?}", id, e);
                                                    shutdown(&id, &client_map_clone, socket).await;
                                                    return;
                                                };
                                            }
                                        }
                                    }
                                    None => {
                                       warn!("Client [{}] receive event result none", id);
                                    }
                                }
                            }
                        }
                    }
                });
            }
            Err(e) => {
                error!("Fail to accept new client connection; err = {:?}", e);
            }
        };
    }
}