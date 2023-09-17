mod event;
mod client;
mod utils;

use std::collections::HashMap;
use log::{error, info, warn};
use std::env;
use serde_derive::Deserialize;
use tokio::fs::File;
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::select;
use crate::client::Client;
use crate::event::Event;
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

    let addr = format!("{}:{}", &file_config.ip, &file_config.port);
    let listener = TcpListener::bind(&addr).await.unwrap_or_else(|err| {
        panic!("Fail to open tcp server; err = {:?}", err);
    });

    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                tokio::spawn(async move {
                    let id = get_id(&addr.ip().to_string(), addr.port());
                    info!("Receive connection from [{}]", id);
                    let mut client = Client::new(id, socket, addr);
                    info!("New client from id [{}]", client.id());

                    // write hello
                    info!("Hello to client {}", client.id());
                    if let Err(e) = client.tcp_stream().write_u8(HELLO_NUM).await {
                        eprintln!("Failed to write hello to [{}]; err = {:?}", client.id(), e);
                        client.shutdown().await;
                        return;
                    }

                    // wait hello
                    match client.tcp_stream().read_u8().await {
                        Ok(n) if n != HELLO_NUM => {
                            warn!("Client [{}] verify hello fail", client.id());
                            client.shutdown().await;
                            return;
                        }
                        Ok(n) => n,
                        Err(e) => {
                            eprintln!("Failed to read hello from [{}]; err = {:?}", client.id(), e);
                            client.shutdown().await;
                            return;
                        }
                    };

                    // 消息缓存
                    let mut b = Vec::new();
                    let mut buf = [0; 1024];
                    info!("Alloc buffer for client [{}]", client.id());

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
                                            info!("Receive get from [{}] len {} content {:?}", client.id(), &key_len, &content);
                                            let event = Event::GET {
                                                key: content,
                                            };
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
                                                info!("Receive set from [{}] len {} key {:?} value {:?}", client.id(), &key_len, &key, &value);
                                                let event = Event::SET {
                                                    key,
                                                    value,
                                                };
                                            }

                                        }
                                    }
                                }
                                n => {
                                    warn!("Unknown op {} from client [{}]", n, client.id());
                                    client.shutdown().await;
                                    return;
                                }
                            }
                        }
                        // 读取消息
                        select! {
                            res = client.tcp_stream().read(&mut buf) => {
                                match res {
                                    Ok(n) => {
                                        if n == 0 {
                                            warn!("Client [{}] read fail", client.id());
                                            client.shutdown().await;
                                            return;
                                        } else {
                                            b.extend_from_slice(&buf[0..n]);
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!("Failed to read from [{}]; err = {:?}", client.id(), e);
                                        return;
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