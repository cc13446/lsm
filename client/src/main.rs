use std::collections::HashMap;
use std::env;
use std::io::BufRead;
use log::{error, info};
use serde_derive::Deserialize;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const SUB: &str = "-";

const HELLO_NUM: u8 = 77;

pub const OP_GET: u8 = 0xc1;
pub const OP_SET: u8 = 0xc2;

pub const RES_GET: u8 = 0x81;
pub const RES_SET: u8 = 0x82;

pub const LEN_MASK: u16 = 0x7fff;
pub const NONE_VALUE_LEN: u16 = 0xffff;

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

// 获取配置文件路径
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
        config_file_path: String::from(get_config_file_path(&args_map, &String::from("./client_config.toml"))),
    };

    info!("LSM client start with config");
    info!("LSM client config file path {}", &env_config.config_file_path);

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

    info!("LSM client file config \n{}", &config_str);

    let file_config: FileConfig = match toml::from_str(&config_str) {
        Ok(s) => s,
        Err(e) => panic!("Error when convert str to config err : {}", e),
    };

    info!("LSM client connect to ip {} port {}", file_config.ip, file_config.port);

    let socket = match TcpStream::connect(format!("{}:{}", file_config.ip, file_config.port)).await {
        Ok(socket) => {
            socket
        }
        Err(e) => {
            panic!("Error conect to server, err = {:?}", e);
        }
    };

    let (mut read_socket, mut write_socket) = socket.into_split();

    let join_read = tokio::spawn(async move {
        let hello = read_socket.read_u8().await.expect("Read hello from server error");
        if hello != HELLO_NUM {
            panic!("Hello fail");
        }

        info!("Alloc buffer");
        let mut buf = [0; 1024];
        let mut b = Vec::new();

        info!("Start read event loop");
        loop {
            let n = read_socket.read(&mut buf).await.expect("Read from server error");
            if n == 0 {
                panic!("Close by server");
            }
            b.extend_from_slice(&buf[0..n]);
            info!("Read from server {:?}", b);
            if b.len() >= 1 {
                let op_res = b[0];
                match op_res {
                    RES_GET => {
                        // 1 bit op res
                        // 2 bit value len
                        // n bit value
                        let value_len = b[1] as usize * 0x100 + b[2] as usize;
                        if value_len == NONE_VALUE_LEN as usize {
                            println!("None");
                        } else {
                            let value_len = value_len & LEN_MASK as usize;
                            if b.len() >= 1 + 2 + value_len {
                                let next = b.split_off(1 + 2 + value_len);
                                let value = b.split_off(1 + 2);
                                b = next;
                                println!("{}", String::from_utf8(value).unwrap_or(String::from("Decoder fail")));
                            }
                        }
                    }
                    RES_SET => {
                        // 1 bit op res
                        b = b.split_off(1);
                    }
                    n => {
                        panic!("Unknown OP res, op = {}", n);
                    }
                }
            }
        }
    });

    let join_write = tokio::spawn(async move {
        info!("Write hello to server");
        write_socket.write_u8(HELLO_NUM).await.expect("Write hello err");
        info!("Start write event loop");
        loop {
            let mut line = String::new();
            std::io::stdin().lock().read_line(&mut line).expect("Read from stdin err");
            line = line.replace("\n", "");
            info!("Read from stdio {}", line);
            let line_split: Vec<&str> = line.split(" ").collect();
            if line_split[0] == "get" && line_split.len() >= 2 {
                write_socket.write_u8(OP_GET).await.expect("Write op get err");
                write_socket.write_u16(line_split[1].len() as u16 & LEN_MASK).await.expect("Write get key len err");
                write_socket.write_all(line_split[1].as_bytes()).await.expect("Write get key err");
            } else if line_split[0] == "set" && line_split.len() >= 3 {
                write_socket.write_u8(OP_SET).await.expect("Write op set err");
                write_socket.write_u16(line_split[1].len() as u16 & LEN_MASK).await.expect("Write set key len err");
                write_socket.write_all(line_split[1].as_bytes()).await.expect("Write set key err");
                write_socket.write_u16(line_split[2].len() as u16 & LEN_MASK).await.expect("Write set value len err");
                write_socket.write_all(line_split[2].as_bytes()).await.expect("Write set value err");
            } else {
                error!("Unknown op {}",line);
            }
        }
    });

    join_read.await.expect("Wait read error");
    join_write.await.expect("Wait write error");
    Ok(())
}
