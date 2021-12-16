use super::parser;
use crate::parser::Response;
use crate::Error;

#[derive(Debug)]
pub enum Message {
    Subscription { channel: String, subscriptions: i64 },
    Unsubscription { channel: String, subscriptions: i64 },
    Message { channel: String, message: String },
    Connected,
    Disconnected(Error),
    Error(Error),
}

#[derive(Debug)]
pub enum ParserError {
    NotString,
    InvalidChannelResponse,
    UnknownType,

    InvalidSubscriptionChannel,
    InvalidSubscriptionCount,

    InvalidUnsubscriptionChannel,
    InvalidUnsubscriptionCount,
}

impl Message {
    /// Parse the response to a message.
    pub fn from_response(res: parser::Response) -> crate::Result<Self> {
        // Make sure the response is a array.
        let arr = match res {
            parser::Response::Array(arr) => Ok(arr),
            _ => Err(Error::ParserError(ParserError::NotString)),
        }?;

        // Get the first element of the array.
        let channel = match arr.get(0) {
            Some(parser::Response::Bulk(channel)) => Ok(channel.as_str()),
            _ => Err(Error::ParserError(ParserError::InvalidChannelResponse)),
        }?;

        // Match on the first element text.
        match channel.to_lowercase().as_str() {
            "subscribe" => Message::from_subscribe(&arr),
            "unsubscribe" => Message::from_unsubscribe(&arr),
            "message" => Message::from_message(&arr),
            _ => Err(Error::ParserError(ParserError::UnknownType)),
        }
    }

    /// parse the subscription message.
    fn from_subscribe(res: &[parser::Response]) -> crate::Result<Self> {
        let channel = match res.get(1) {
            Some(parser::Response::Bulk(channel)) => Ok((*channel).clone()),
            _ => Err(Error::ParserError(ParserError::InvalidSubscriptionChannel)),
        }?;

        let subscriptions = match res.get(2) {
            Some(parser::Response::Integer(subscriptions)) => Ok(*subscriptions),
            _ => Err(Error::ParserError(ParserError::InvalidSubscriptionCount)),
        }?;

        Ok(Self::Subscription {
            channel,
            subscriptions,
        })
    }

    /// parse the unsubscription message.
    fn from_unsubscribe(res: &[parser::Response]) -> crate::Result<Self> {
        let channel = match res.get(1) {
            Some(parser::Response::Bulk(channel)) => Ok((*channel).clone()),
            _ => Err(Error::ParserError(
                ParserError::InvalidUnsubscriptionChannel,
            )),
        }?;

        let subscriptions = match res.get(2) {
            Some(parser::Response::Integer(subscriptions)) => Ok(*subscriptions),
            _ => Err(Error::ParserError(ParserError::InvalidUnsubscriptionCount)),
        }?;

        Ok(Self::Unsubscription {
            channel,
            subscriptions,
        })
    }

    /// parse the response to a message.
    fn from_message(res: &[parser::Response]) -> crate::Result<Self> {
        let channel = match res.get(1) {
            Some(parser::Response::Bulk(channel)) => Ok((*channel).clone()),
            _ => Err(Error::ParserError(ParserError::InvalidSubscriptionChannel)),
        }?;

        let message = match res.get(2) {
            Some(parser::Response::Bulk(message)) => Ok((*message).clone()),
            _ => Err(Error::ParserError(ParserError::InvalidSubscriptionCount)),
        }?;

        Ok(Self::Message { channel, message })
    }
}
