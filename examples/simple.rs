use std::{sync::Arc, time::Duration};

use redis_subscribe::RedisSub;
use tokio::time::sleep;
use tokio_stream::StreamExt;

#[tokio::main]
pub async fn main() {
    // Connect to the Redis server.
    let sub = Arc::new(RedisSub::new("localhost:6379"));
    let redis_listener = sub.clone();

    // Listen for incomming messages.
    tokio::spawn(async move {
        let mut stream = redis_listener
            .listen()
            .await
            .expect("failed to connect to Redis");

        // Use a stream to loop trough all messages.
        while let Some(msg) = stream.next().await {
            println!("got = {:?}", msg);
        }
    });

    // Clone the connection, in order to move them to the tasks.
    let channel3_sub = sub.clone();
    let channel4_sub = sub.clone();

    // Subscribe to four channels.
    sub.subscribe("channel1".to_string()).await;
    sub.subscribe("channel2".to_string()).await;
    tokio::spawn(async move { channel3_sub.subscribe("channel3".to_string()).await });
    tokio::spawn(async move { channel4_sub.subscribe("channel4".to_string()).await });

    // Sleep for 5 seconds.
    sleep(Duration::from_millis(5 * 1000)).await;

    // Unsubscribe from the first channel.
    sub.unsubscribe("channel1".to_string()).await;

    // Sleep for 10 seconds, afterwards program will exit.
    sleep(Duration::from_millis(10 * 1000)).await;
}
