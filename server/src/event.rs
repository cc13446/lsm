pub const OP_GET: u8 = 0xc1;
pub const OP_SET: u8 = 0xc2;

pub const LEN_MASK: u16 = 0x7fff;

pub enum Event {
    GET {
        key: Vec<u8>
    },
    SET {
        key: Vec<u8>,
        value: Vec<u8>,
    },
}

