use super::parser;
use crate::Error;

#[derive(Debug)]
pub enum Message {
    Subscription {
        channel: String,
        subscriptions: i64,
    },
    Unsubscription {
        channel: String,
        subscriptions: i64,
    },
    Message {
        channel: String,
        message: String,
    },

    PatternSubscription {
        channel: String,
        subscriptions: i64,
    },
    PatternUnsubscription {
        channel: String,
        subscriptions: i64,
    },
    PatternMessage {
        pattern: String,
        channel: String,
        message: String,
    },
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
    InvalidSubscriptionPattern,

    InvalidUnsubscriptionChannel,
    InvalidUnsubscriptionCount,
}

impl Message {
    /// Parse the response to a message.
    ///
    /// # Errors
    /// Returns an error if the response has unexpected types.
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
            "subscribe" => Self::from_subscribe(&arr),
            "unsubscribe" => Self::from_unsubscribe(&arr),
            "message" => Self::from_message(&arr),
            "pmessage" => Self::from_pmessage(&arr),
            "psubscribe" => Self::from_psubscribe(&arr),
            "punsubscribe" => Self::from_punsubscribe(&arr),
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

    fn from_psubscribe(res: &[parser::Response]) -> crate::Result<Self> {
        let channel = match res.get(1) {
            Some(parser::Response::Bulk(channel)) => Ok((*channel).clone()),
            _ => Err(Error::ParserError(ParserError::InvalidSubscriptionChannel)),
        }?;

        let subscriptions = match res.get(2) {
            Some(parser::Response::Integer(subscriptions)) => Ok(*subscriptions),
            _ => Err(Error::ParserError(ParserError::InvalidSubscriptionCount)),
        }?;

        Ok(Self::PatternSubscription {
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

    fn from_punsubscribe(res: &[parser::Response]) -> crate::Result<Self> {
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

        Ok(Self::PatternUnsubscription {
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

    /// parse the response to a pattern message
    fn from_pmessage(res: &[parser::Response]) -> crate::Result<Self> {
        let pattern = match res.get(1) {
            Some(parser::Response::Bulk(pattern)) => Ok((*pattern).clone()),
            _ => Err(Error::ParserError(ParserError::InvalidSubscriptionPattern)),
        }?;

        let channel = match res.get(2) {
            Some(parser::Response::Bulk(channel)) => Ok((*channel).clone()),
            _ => Err(Error::ParserError(ParserError::InvalidSubscriptionChannel)),
        }?;

        let message = match res.get(3) {
            Some(parser::Response::Bulk(message)) => Ok((*message).clone()),
            _ => Err(Error::ParserError(ParserError::InvalidSubscriptionCount)),
        }?;

        Ok(Self::PatternMessage {
            pattern,
            channel,
            message,
        })
    }
}

impl Message {
    #[must_use]
    #[inline]
    pub const fn is_subscription(&self) -> bool {
        matches!(self, Self::Subscription { .. })
    }

    #[must_use]
    #[inline]
    pub const fn is_pattern_subscription(&self) -> bool {
        matches!(self, Self::PatternSubscription { .. })
    }

    #[must_use]
    #[inline]
    pub const fn is_unsubscription(&self) -> bool {
        matches!(self, Self::Unsubscription { .. })
    }

    #[must_use]
    #[inline]
    pub const fn is_pattern_unsubscription(&self) -> bool {
        matches!(self, Self::PatternUnsubscription { .. })
    }

    #[must_use]
    #[inline]
    pub const fn is_message(&self) -> bool {
        matches!(self, Self::Message { .. })
    }

    #[must_use]
    #[inline]
    pub const fn is_pattern_message(&self) -> bool {
        matches!(self, Self::PatternMessage { .. })
    }

    #[must_use]
    #[inline]
    pub const fn is_connected(&self) -> bool {
        matches!(self, Self::Connected)
    }

    #[must_use]
    #[inline]
    pub const fn is_disconnected(&self) -> bool {
        matches!(self, Self::Disconnected(_))
    }

    #[must_use]
    #[inline]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Error(_))
    }
}
