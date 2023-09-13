mod event_provider;
mod event;

use std::collections::HashMap;
use log::{error, info};
use std::env;
use std::io::Error;
use serde_derive::Deserialize;
use tokio::fs::File;
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const SUB: &str = "-";

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
            Ok((mut socket, addr)) => {
                tokio::spawn(async move {
                    info!("Receive connection from {}:{}", addr.ip(), addr.port());

                    let mut buf = [0; 1024];

                    // In a loop, read data from the socket and write the data back.
                    loop {
                        let n = match socket.read(&mut buf).await {
                            // socket closed
                            Ok(n) if n == 0 => return,
                            Ok(n) => n,
                            Err(e) => {
                                eprintln!("failed to read from socket; err = {:?}", e);
                                return;
                            }
                        };

                        // Write the data back
                        if let Err(e) = socket.write_all(&buf[0..n]).await {
                            eprintln!("failed to write to socket; err = {:?}", e);
                            return;
                        }
                    }
                });
            },
            Err(e) => {
                error!("Fail to accept new client connection; err = {:?}", e);
            },
        };
    }
}