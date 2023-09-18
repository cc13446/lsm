use log::error;
use tokio::sync::mpsc::Sender;
use crate::event::EventRes;

pub struct Client {
    id: String,
    sender: Sender<EventRes>,
}

impl Client {
    pub fn new(id: String, sender: Sender<EventRes>) -> Client {
        Client {
            id,
            sender,
        }
    }

    pub async fn send_event_res(&mut self, event_res: EventRes) {
        if let Err(e) =  self.sender.send(event_res).await {
            error!("Send event res fail, id = {}, err => {:?}", self.id, e);
        };
    }
}