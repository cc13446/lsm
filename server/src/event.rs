enum Event {
    GET {
        key: Vec<u8>
    },
    SET {
        key: Vec<u8>,
        value: Vec<u8>,
    },
}

