#[derive(Debug)]
pub enum Command {
    Subscribe(String),
    Unsubscribe(String),
    PatternSubscribe(String),
    PatternUnsubscribe(String),
}

impl Command {
    pub fn to_string(&self) -> String {
        match self {
            Command::Subscribe(channel) => format!("SUBSCRIBE {}\r\n", channel),
            Command::Unsubscribe(channel) => format!("UNSUBSCRIBE {}\r\n", channel),
            Command::PatternSubscribe(channel) => format!("PSUBSCRIBE {}\r\n", channel),
            Command::PatternUnsubscribe(channel) => format!("PUNSUBSCRIBE {}\r\n", channel),
        }
    }
}
