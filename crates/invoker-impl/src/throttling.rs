// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;

use futures::{FutureExt, StreamExt, stream::Repeat};
use gardal::{
    AtomicSharedStorage, Clock, StreamExt as GardalStreamExt, TokioClock, futures::ThrottledStream,
};

pub type TokenBucket<C = TokioClock> = gardal::TokenBucket<AtomicSharedStorage, C>;

/// Token type used by the throttling system.
/// Currently a unit type since we only care about token availability, not token content.
type Token = ();

pin_project_lite::pin_project! {
    #[project = RateLimiterInnerProj]

    /// Internal enum representing the two throttling modes:
    /// - `Unlimited`: No rate limiting applied
    /// - `Limited`: Rate limited using a token bucket stream
    enum RateLimiterInner<C> where C: Clock {
        /// No rate limiting - operations proceed without delay
        Unlimited,
        /// Rate limited using a token bucket stream
        Limited{#[pin] stream:ThrottledStream<Repeat<()>, AtomicSharedStorage, C>},
    }
}

pin_project_lite::pin_project! {
    /// A rate limiter that can operate in either limited or unlimited mode.
    ///
    /// The throttler uses a token bucket algorithm to control the rate of operations.
    /// It maintains an internal token that must be available for operations to proceed.
    ///
    ///  Modes
    /// - **Limited**: Uses a token bucket to enforce rate limits
    /// - **Unlimited**: No rate limiting, always ready
    ///
    ///  Key Methods
    /// - `is_ready()`: Check if operations can proceed
    /// - `consume()`: Consume a token (for limited mode)
    /// - `next()`: Drive the token stream forward
    pub(crate) struct Throttler<C = TokioClock> where C: Clock {
        #[pin]
        stream: ThrottledStream<Repeat<()>, AtomicSharedStorage, C>,
        token: Option<Token>,
    }
}

impl Throttler<TokioClock> {
    pub fn unlimited() -> Self {
        Self::new(None)
    }
}

impl<C> Throttler<C>
where
    C: Clock,
{
    /// Creates a throttler with rate limiting enabled.
    ///
    /// # Arguments
    /// * `bucket` - The token bucket that defines the rate and burst limits
    ///
    /// # Returns
    /// A new throttler that will enforce the bucket's rate limits
    pub fn new(bucket: impl Into<Option<TokenBucket<C>>>) -> Self {
        // Create a rate-limited stream that produces tokens at the bucket's rate
        let mut stream = futures::stream::repeat(()).throttle(bucket);

        let token = stream.next().now_or_never().flatten();
        Self { stream, token }
    }

    /// Consumes a token from the throttler.
    ///
    /// This method should be called when an operation is about to be executed.
    /// For limited throttlers, it removes the current token and attempts to get the next one.
    /// For unlimited throttlers, this is a no-op.
    ///
    /// Calling this method when the throttler is not ready will panic.
    ///
    /// # Arguments
    /// * `self` - Mutable pinned reference to self
    pub fn consume(mut self: Pin<&mut Self>) {
        let mut this = self.as_mut().project();
        // Remove current token
        this.token.take().expect("token was not available");
        // Try to get next token immediately (non-blocking)
        *this.token = this.stream.next().now_or_never().flatten();
    }

    /// Checks if the throttler is ready to allow an operation.
    ///
    /// # Returns
    /// `true` if an operation can proceed, `false` if it should wait
    pub fn is_ready(&self) -> bool {
        self.token.is_some()
    }

    /// Drives the token stream forward to replenish tokens.
    ///
    /// This method should be called periodically to ensure the token stream progresses
    /// and new tokens become available. For unlimited throttlers, this is a no-op.
    ///
    /// # Arguments
    /// * `self` - Mutable pinned reference to self
    ///
    /// # Returns
    /// Always returns `true` (for compatibility with stream operations)
    pub async fn next(self: Pin<&mut Self>) -> bool {
        let mut this = self.project();
        // If no token is available, wait for the next one from the stream
        if this.token.is_none() {
            *this.token = this.stream.next().await;
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{num::NonZeroU32, sync::Arc};

    #[tokio::test]
    async fn test_unlimited_throttler() {
        let throttler = Throttler::<TokioClock>::new(None);
        assert!(throttler.is_ready());

        let mut pinned = std::pin::pin!(throttler);
        pinned.as_mut().consume();
        assert!(pinned.is_ready());

        let result = pinned.as_mut().next().now_or_never().expect("always ready");
        assert!(result);
        assert!(pinned.is_ready());
    }

    #[tokio::test]
    async fn test_limited_throttler() {
        let bucket = TokenBucket::from_parts(
            gardal::Limit::per_second_and_burst(
                NonZeroU32::new(10).unwrap(),
                NonZeroU32::new(5).unwrap(),
            ), // 10 per second, burst of 5
            gardal::TokioClock::default(),
        );
        bucket.add_tokens(5); // Fill the bucket

        let throttler = Throttler::new(bucket);
        assert!(throttler.is_ready());

        let mut pinned = std::pin::pin!(throttler);
        pinned.as_mut().consume();

        // Should still be ready since we had 5 tokens initially
        assert!(pinned.is_ready());

        // Drive the stream forward
        let result = pinned.next().await;
        assert!(result);
    }

    #[tokio::test]
    async fn test_throttler_token_consumption() {
        let clock = Arc::new(gardal::ManualClock::default());

        let bucket = TokenBucket::from_parts(
            gardal::Limit::per_second_and_burst(
                NonZeroU32::new(1).unwrap(),
                NonZeroU32::new(1).unwrap(),
            ), // 1 per second, burst of 1
            Arc::clone(&clock),
        );
        bucket.add_tokens(1);

        let throttler = Throttler::new(bucket);
        let mut pinned = std::pin::pin!(throttler);
        // assert!(throttler.is_ready());

        while pinned.is_ready() {
            pinned.as_mut().consume();
        }

        clock.advance(1.0);

        // Drive the stream forward to get the new token
        pinned.as_mut().next().await;
        assert!(pinned.is_ready());
    }
}
