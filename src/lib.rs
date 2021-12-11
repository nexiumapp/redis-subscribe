mod command;
mod message;
mod parser;

use std::cmp;
use std::collections::HashSet;
use std::time::Duration;

use async_stream::stream;
use rand::{thread_rng, Rng};
use tokio::io::AsyncReadExt;
use tokio::time::sleep;
use tokio::{
    io::AsyncWriteExt,
    net::{tcp::OwnedWriteHalf, TcpStream},
    sync::Mutex,
};
use tokio_stream::Stream;

use crate::command::Command;
pub use crate::message::Message;

/// Redis subscription object.
/// This connects to the Redis server.
#[derive(Debug)]
pub struct RedisSub {
    /// Address of the redis server.
    addr: String,
    /// Set of channels currently subscribed to.
    channels: Mutex<HashSet<String>>,
    /// TCP socket writer to write commands to.
    writer: Mutex<Option<OwnedWriteHalf>>,
}

impl RedisSub {
    /// Create the new Redis client.
    /// This does not connect to the server, use `.listen()` for that.
    pub fn new(addr: &str) -> Self {
        Self {
            addr: addr.to_string(),
            channels: Mutex::new(HashSet::new()),
            writer: Mutex::new(None),
        }
    }

    /// Subscribe to a channel.
    pub async fn subscribe(&self, channel: String) -> bool {
        self.channels.lock().await.insert(channel.clone());

        self.send_cmd(Command::Subscribe(channel)).await.is_ok()
    }

    /// Unsubscribe from a channel.
    pub async fn unsubscribe(&self, channel: String) -> bool {
        if !self.channels.lock().await.remove(&channel) {
            return false;
        }

        self.send_cmd(Command::Unsubscribe(channel)).await.is_ok()
    }

    /// Listen for incoming messages.
    /// Only here the server connects to the Redis server.
    /// It handles reconnection and backoff for you.
    pub async fn listen(&self) -> impl Stream<Item = Message> + '_ {
        let mut retry_count = 0;

        Box::pin(stream! {
            loop {
                // Generate jitter for the backoff function.
                let jitter = thread_rng().gen_range(0..1000);
                // Connect to the Redis server.
                let (mut read, write) = match TcpStream::connect(self.addr.as_str()).await {
                    Ok(stream) => stream.into_split(),
                    Err(_) => {
                        // Backoff and reconnect.
                        retry_count += 1;
                        let timeout = cmp::min(retry_count^2, 64) * 1000 + jitter;
                        sleep(Duration::from_millis(timeout)).await;
                        continue;
                    },
                };

                // Reset the retry counter.
                retry_count = 0;

                // Update the stored writer.
                let mut stored_writer = self.writer.lock().await;
                *stored_writer = Some(write);
                drop(stored_writer);

                let mut errored = false;
                for channel in self.channels.lock().await.iter() {
                    // Subscribe to all channels requested.
                    match self.send_cmd(Command::Subscribe(channel.to_string())).await {
                        Ok(_) => (),
                        Err(_) => {
                            errored = true;
                        }
                    }
                }

                // Disconnect and reconnect if the subscriptions errored.
                if errored {
                    continue;
                }

                // Yield a connect message to the library consumer.
                yield Message::Connected;

                // Create the read buffers.
                let mut buf = [0; 64 * 1024];
                let mut unread_buf = String::new();

                loop {
                    // Read incomming data to the buffer.
                    let res = match read.read(&mut buf).await {
                        Ok(0) => Err(()),
                        Ok(n) => Ok(n),
                        Err(_) => Err(()),
                    };

                    /// Disconnect and reconnect if a write error occured.
                    let n = match res {
                        Ok(n) => n,
                        Err(_) => {
                            *self.writer.lock().await = None;
                            yield Message::Disconnected;
                            sleep(Duration::from_millis(500 + jitter)).await;
                            break;
                        }
                    };

                    // Add the new data to the unread buffer.
                    unread_buf.push_str(std::str::from_utf8(&buf[..n]).unwrap());
                    // Parse the unread data.
                    let parsed = parser::parse(&mut unread_buf);

                    // Loop through the parsed commands.
                    for res in parsed {
                        // Create a message from the parsed command and yield it.
                        match Message::from_response(res) {
                            Ok(msg) => yield msg,
                            Err(_) => continue,
                        };
                    }
                }
            }
        })
    }

    /// Send a command to the server.
    async fn send_cmd(&self, command: Command) -> Result<(), String> {
        match &mut *self.writer.lock().await {
            Some(writer) => {
                if writer.writable().await.is_err() {
                    return Ok(());
                }

                match writer.write_all(command.to_string().as_bytes()).await {
                    Ok(_) => Ok(()),
                    Err(_) => Err(String::from("Failed to send message.")),
                }
            }
            None => Ok(()),
        }
    }
}
