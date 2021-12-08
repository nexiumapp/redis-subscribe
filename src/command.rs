#[derive(Debug)]
pub enum Command {
    Subscribe(String),
    Unsubscribe(String),
}

impl Command {
    pub fn to_string(&self) -> String {
        match self {
            Command::Subscribe(channel) => format!("SUBSCRIBE {}\r\n", channel),
            Command::Unsubscribe(channel) => format!("UNSUBSCRIBE {}\r\n", channel),
        }
    }
}
