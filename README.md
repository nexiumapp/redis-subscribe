# Redis subscription

Easily subscribe and unsubscribe to redis pubsub.

## Why this package?

Currently the [redis](https://crates.io/crates/redis) crate does not allow to subscribe or unsubscribe after you attached a listener, as you can [see in this issue](https://github.com/mitsuhiko/redis-rs/issues/509).
This crate aims to resolve this.

## What doesn't it do?

Almost everything! It only handles subscriptions, so it cannot set/get keys or even publishing.
Use the [redis](https://crates.io/crates/redis) crate instead, this one works well with all other features.

## Usage

Take a look at the example folder to see usage examples.
