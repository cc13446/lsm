use std::collections::HashMap;
use std::net::SocketAddr;
use log::info;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

pub struct Client {
    id: String,
    addr: SocketAddr,
    tcp_stream: TcpStream,
}

impl Client {
    pub fn new(id: String, tcp_stream: TcpStream, addr: SocketAddr) -> Client {
        Client {
            id,
            tcp_stream,
            addr,
        }
    }


    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn tcp_stream(&mut self) -> & mut TcpStream {
        &mut self.tcp_stream
    }
    pub fn addr(&self) -> &SocketAddr {
        &self.addr
    }

    pub async fn shutdown(& mut self) {
        info!("Client [{}] disconnect", self.id());
        self.tcp_stream().shutdown().await.unwrap_or_else(|e| {
            info!("Fail close client [{}]; err = {:?} ", self.id(), e);
        });
    }
}


pub struct ClientManager {
    map: HashMap<String, Client>,
}


impl ClientManager {
    pub fn add_new_client(&mut self, client: Client) {
        self.map.insert(client.id.clone(), client);
    }

    pub fn remove_client(&mut self, id: String) {
        self.map.remove(id.as_str());
    }
}