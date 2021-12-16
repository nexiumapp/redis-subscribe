mod command;
mod error;
mod message;
mod parser;

#[macro_use]
extern crate tracing;

use std::cmp;
use std::collections::HashSet;
use std::time::Duration;

use async_stream::stream;
use rand::{thread_rng, Rng};
use tokio::io::AsyncReadExt;
use tokio::time::sleep;
use tokio::{
    io::AsyncWriteExt,
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    sync::Mutex,
};
use tokio_stream::Stream;

use crate::command::Command;
pub use crate::error::*;
pub use crate::message::Message;

/// Redis subscription object.
/// This connects to the Redis server.
#[derive(Debug)]
pub struct RedisSub {
    /// Address of the redis server.
    addr: String,
    /// Set of channels currently subscribed to.
    channels: Mutex<HashSet<String>>,
    /// Set of channels currently subscribed to by pattern.
    pattern_channels: Mutex<HashSet<String>>,
    /// TCP socket writer to write commands to.
    writer: Mutex<Option<OwnedWriteHalf>>,
}

impl RedisSub {
    /// Create the new Redis client.
    /// This does not connect to the server, use `.listen()` for that.
    #[must_use]
    pub fn new(addr: &str) -> Self {
        Self {
            addr: addr.to_string(),
            channels: Mutex::new(HashSet::new()),
            pattern_channels: Mutex::new(HashSet::new()),
            writer: Mutex::new(None),
        }
    }

    /// Subscribe to a channel.
    pub async fn subscribe(&self, channel: String) -> crate::Result<()> {
        self.channels.lock().await.insert(channel.clone());

        self.send_cmd(Command::Subscribe(channel)).await
    }

    /// Unsubscribe from a channel.
    pub async fn unsubscribe(&self, channel: String) -> crate::Result<()> {
        if !self.channels.lock().await.remove(&channel) {
            return Err(crate::Error::NotSubscribed);
        }

        self.send_cmd(Command::Unsubscribe(channel)).await
    }

    /// Subscribe to a pattern of channels.
    pub async fn psubscribe(&self, channel: String) -> crate::Result<()> {
        self.channels.lock().await.insert(channel.clone());

        self.send_cmd(Command::PatternSubscribe(channel)).await
    }

    /// Unsubscribe from a pattern of channels.
    pub async fn punsubscribe(&self, channel: String) -> crate::Result<()> {
        if !self.channels.lock().await.remove(&channel) {
            return Err(crate::Error::NotSubscribed);
        }

        self.send_cmd(Command::PatternUnsubscribe(channel)).await
    }

    /// Connect to the Redis server specified by `self.addr`.
    ///
    /// Handles exponential backoff.
    ///
    /// Returns a split TCP stream.
    ///
    /// # Errors
    /// Returns an error if attempting connection failed eight times.
    pub(crate) async fn connect(&self) -> crate::Result<(OwnedReadHalf, OwnedWriteHalf)> {
        let mut retry_count = 0;

        loop {
            // Generate jitter for the backoff function.
            let jitter = thread_rng().gen_range(0..1000);
            // Connect to the Redis server.
            match TcpStream::connect(self.addr.as_str()).await {
                Ok(stream) => return Ok(stream.into_split()),
                Err(_) if retry_count <= 7 => {
                    // Backoff and reconnect.
                    retry_count += 1;
                    let timeout = cmp::min(retry_count ^ 2, 64) * 1000 + jitter;
                    sleep(Duration::from_millis(timeout)).await;
                    continue;
                }
                Err(e) => {
                    // Retry count has passed 7.
                    // Assume connection failed and return.
                    return Err(crate::Error::IoError(e));
                }
            };
        }
    }

    async fn subscribe_stored(&self) -> crate::Result<()> {
        for channel in self.channels.lock().await.iter() {
            self.send_cmd(Command::Subscribe(channel.to_string()))
                .await?
        }

        for channel in self.pattern_channels.lock().await.iter() {
            self.send_cmd(Command::PatternSubscribe(channel.to_string()))
                .await?
        }

        Ok(())
    }

    /// Listen for incoming messages.
    /// Only here the server connects to the Redis server.
    /// It handles reconnection and backoff for you.
    pub async fn listen(&self) -> impl Stream<Item = Message> + '_ {
        Box::pin(stream! {
            loop {
                let (mut read, write) = match self.connect().await {
                    Ok(t) => t,
                    Err(e) => {
                        warn!("failed to connect to server: {:?}", e);
                        continue;
                    }
                };

                // Update the stored writer.
                {
                    let mut stored_writer = self.writer.lock().await;
                    *stored_writer = Some(write);
                }

                // Subscribe to all stored channels
                if let Err(e) = self.subscribe_stored().await {
                    warn!("failed to subscribe to stored channels on connection, trying connection again... (err {:?})", e);
                    continue;
                }

                // Yield a connect message to the library consumer.
                yield Message::Connected;

                // Create the read buffers.
                let mut buf = [0; 64 * 1024];
                let mut unread_buf = String::new();

                'inner: loop {
                    // Read incoming data to the buffer.
                    let res = match read.read(&mut buf).await {
                        Ok(0) => Err(crate::Error::ZeroBytesRead),
                        Ok(n) => Ok(n),
                        Err(e) => Err(crate::Error::from(e)),
                    };

                    // Disconnect and reconnect if a write error occurred.
                    let n = match res {
                        Ok(n) => n,
                        Err(e) => {
                            *self.writer.lock().await = None;
                            yield Message::Disconnected(e);
                            break 'inner;
                        }
                    };

                    let buf_data = match std::str::from_utf8(&buf[..n]) {
                        Ok(d) => d,
                        Err(e) => {
                            yield Message::Error(e.into());
                            continue;
                        }
                    };

                    // Add the new data to the unread buffer.
                    unread_buf.push_str(buf_data);
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
    async fn send_cmd(&self, command: Command) -> crate::Result<()> {
        if let Some(writer) = &mut *self.writer.lock().await {
            writer.writable().await?;

            debug!("sending command {:?} to redis", &command);
            writer.write_all(command.to_string().as_bytes()).await?;
        }

        Ok(())
    }
}
