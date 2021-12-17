use crate::{parser, Command, Message};
use async_stream::stream;
use rand::{thread_rng, Rng};
use std::cmp;
use std::collections::HashSet;
use std::time::Duration;
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
    ///
    /// # Errors
    /// Returns an error if an error happens on the underlying TCP stream.
    pub async fn subscribe(&self, channel: String) -> crate::Result<()> {
        self.channels.lock().await.insert(channel.clone());

        self.send_cmd(Command::Subscribe(channel)).await
    }

    /// Unsubscribe from a channel.
    ///
    /// # Errors
    /// Returns an error if an error happens on the underlying TCP stream.
    pub async fn unsubscribe(&self, channel: String) -> crate::Result<()> {
        if !self.channels.lock().await.remove(&channel) {
            return Err(crate::Error::NotSubscribed);
        }

        self.send_cmd(Command::Unsubscribe(channel)).await
    }

    /// Subscribe to a pattern of channels.
    ///
    /// # Errors
    /// Returns an error if an error happens on the underlying TCP stream.
    pub async fn psubscribe(&self, channel: String) -> crate::Result<()> {
        self.pattern_channels.lock().await.insert(channel.clone());

        self.send_cmd(Command::PatternSubscribe(channel)).await
    }

    /// Unsubscribe from a pattern of channels.
    ///
    /// # Errors
    /// Returns an error if an error happens on the underlying TCP stream.
    pub async fn punsubscribe(&self, channel: String) -> crate::Result<()> {
        if !self.pattern_channels.lock().await.remove(&channel) {
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
                    warn!(
                        "failed to connect to redis (attempt {}/8) {:?}",
                        retry_count, e
                    );
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
                .await?;
        }

        for channel in self.pattern_channels.lock().await.iter() {
            self.send_cmd(Command::PatternSubscribe(channel.to_string()))
                .await?;
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
                    debug!("updating stored Redis TCP writer");
                    let mut stored_writer = self.writer.lock().await;
                    *stored_writer = Some(write);
                }

                // Subscribe to all stored channels
                debug!("subscribing to stored channels after connect");
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
                    debug!("reading incoming data");
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
                        debug!("new message");
                        // Create a message from the parsed command and yield it.
                        match Message::from_response(res) {
                            Ok(msg) => yield msg,
                            Err(e) => {
                                warn!("failed to parse message: {:?}", e);
                                continue;
                            },
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

#[cfg(test)]
mod tests {
    use super::*;
    use redis::AsyncCommands;
    use tokio_stream::StreamExt;

    async fn get_redis_connections() -> (redis::Client, redis::aio::Connection, RedisSub) {
        println!("opening redis connections");
        let client =
            redis::Client::open("redis://127.0.0.1/").expect("failed to create Redis client");
        let connection = client
            .get_tokio_connection()
            .await
            .expect("failed to open Redis connection");
        let redis_sub = RedisSub::new("127.0.0.1:6379");
        (client, connection, redis_sub)
    }

    #[tokio::test]
    #[ignore]
    async fn test_redis_sub() {
        let (client, mut connection, redis_sub) = get_redis_connections().await;

        println!("subscribing to new redis channel");
        redis_sub
            .subscribe("1234".to_string())
            .await
            .expect("failed to subscribe to new Redis channel");
        println!("spawning background future");
        let f = tokio::spawn(async move {
            {
                println!("listening to redis subscriber");
                let mut stream = redis_sub.listen().await;

                println!("waiting for Redis connection to succeed");
                let msg = tokio::time::timeout(Duration::from_millis(500), stream.next())
                    .await
                    .expect("timeout duration of 500 milliseconds was exceeded")
                    .expect("expected a Message");
                assert!(
                    msg.is_connected(),
                    "message after opening stream was not `Connected`: {:?}",
                    msg
                );

                println!("waiting for Redis subscription to be returned");
                let msg = tokio::time::timeout(Duration::from_millis(500), stream.next())
                    .await
                    .expect("timeout duration of 500 milliseconds was exceeded")
                    .expect("expected a Message");
                assert!(
                    msg.is_subscription(),
                    "message after connection was not `Subscription`: {:?}",
                    msg
                );

                println!("waiting for Redis message");
                let msg = tokio::time::timeout(Duration::from_secs(2), stream.next())
                    .await
                    .expect("timeout duration of 2 seconds was exceeded")
                    .expect("expected a Message");
                assert!(
                    msg.is_message(),
                    "message after subscription was not `Message`: {:?}",
                    msg
                );
                match msg {
                    Message::Message { channel, message } => {
                        assert_eq!(channel, "1234".to_string());
                        assert_eq!(message, "1234".to_string());
                    }
                    _ => unreachable!("already checked this is message"),
                }
            }

            redis_sub
        });

        tokio::time::sleep(Duration::from_secs(1)).await;
        connection
            .publish::<&str, &str, u32>("1234", "1234")
            .await
            .expect("failed to send publish command to Redis");
        let redis_sub = f.await.expect("background future failed");

        redis_sub
            .unsubscribe("1234".to_string())
            .await
            .expect("failed to unsubscribe from Redis channel");
    }

    #[tokio::test]
    #[ignore]
    pub async fn test_redis_pattern_sub() {
        let (client, mut connection, redis_sub) = get_redis_connections().await;

        println!("subscribing to new redis channel");
        redis_sub
            .psubscribe("*1234*".to_string())
            .await
            .expect("failed to subscribe to new Redis channel");
        println!("spawning background future");
        let f = tokio::spawn(async move {
            {
                println!("listening to redis subscriber");
                let mut stream = redis_sub.listen().await;

                println!("waiting for Redis connection to succeed");
                let msg = tokio::time::timeout(Duration::from_millis(500), stream.next())
                    .await
                    .expect("timeout duration of 500 milliseconds was exceeded")
                    .expect("expected a Message");
                assert!(
                    msg.is_connected(),
                    "message after opening stream was not `Connected`: {:?}",
                    msg
                );

                println!("waiting for Redis subscription to be returned");
                let msg = tokio::time::timeout(Duration::from_millis(500), stream.next())
                    .await
                    .expect("timeout duration of 500 milliseconds was exceeded")
                    .expect("expected a Message");
                assert!(
                    msg.is_subscription(),
                    "message after connection was not `Subscription`: {:?}",
                    msg
                );

                println!("waiting for Redis message");
                let msg = tokio::time::timeout(Duration::from_secs(2), stream.next())
                    .await
                    .expect("timeout duration of 2 seconds was exceeded")
                    .expect("expected a Message");
                assert!(
                    msg.is_pattern_message(),
                    "message after subscription was not `PatternMessage`: {:?}",
                    msg
                );
                match msg {
                    Message::PatternMessage {
                        pattern,
                        channel,
                        message,
                    } => {
                        assert_eq!(pattern, "*1234*".to_string());
                        assert_eq!(channel, "012345".to_string());
                        assert_eq!(message, "123456".to_string());
                    }
                    _ => unreachable!("already checked this is message"),
                }
            }

            redis_sub
        });

        tokio::time::sleep(Duration::from_secs(1)).await;
        connection
            .publish::<&str, &str, u32>("012345", "123456")
            .await
            .expect("failed to send publish command to Redis");
        let redis_sub = f.await.expect("background future failed");

        redis_sub
            .punsubscribe("*1234*".to_string())
            .await
            .expect("failed to unsubscribe from Redis channel");
    }
}
