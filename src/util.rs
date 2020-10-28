use rand::Rng;

pub fn new_id() -> String {
    hex::encode(rand::thread_rng().gen::<[u8; 16]>())
}
