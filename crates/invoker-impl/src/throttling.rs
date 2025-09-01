// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! # Invoker Throttling System
//!
//! This module provides a rate limiting mechanism for the Restate invoker service to control
//! the rate at which invocations and actions are processed. The throttling system uses a
//! token bucket algorithm to enforce rate limits while allowing for burst capacity.
//!
//! ## Overview
//!
//! The invoker service uses two separate throttlers:
//! - **Invocation Throttler**: Controls the rate at which new invocations are started
//! - **Action Throttler**: Controls the rate at which actions (like processing invocation task outputs) are executed
//!
//! ## How It Works
//!
//! 1. **Token Bucket**: Each throttler uses a token bucket with configurable rate and burst capacity
//! 2. **Rate Limiting**: Tokens are consumed at a fixed rate (e.g., 100k per second)
//! 3. **Burst Handling**: The bucket can hold up to the burst capacity (e.g., 1M tokens)
//! 4. **Throttling Logic**: Operations are only allowed when tokens are available
//!
//! ## Configuration
//!
//! Throttling is configured through the `InvokerOptions`:
//! - `invocation_throttling`: Controls invocation start rate
//! - `action_throttling`: Controls action processing rate
//!
//! Each can specify:
//! - `rate`: Tokens per second (default: 100k)
//! - `burst`: Maximum token capacity (default: 1M)
//!
//! ## Usage in the Service
//!
//! The throttlers are integrated into the main service loop:
//! - Invocations are only dequeued when the invocation throttler has tokens available
//! - Actions are only processed when the action throttler has tokens available
//! - The service gracefully handles both limited and unlimited throttling modes
//!
//! ## Example Flow
//!
//! 1. Service receives invocation request
//! 2. Checks if invocation throttler has tokens (`is_ready()`)
//! 3. If ready, consumes token and processes invocation
//! 4. If not ready, waits for token replenishment
//! 5. Similar flow for action processing
//!
//! This ensures the service can handle high load while maintaining predictable resource usage
//! and preventing system overload.

use std::{
    iter::{self, Repeat},
    pin::Pin,
};

use futures::{FutureExt, StreamExt, stream::Iter};
use gardal::{
    AtomicStorage, RateLimitedStreamExt, TokenBucket, TokioClock, futures::RateLimitedStream,
};

/// Token type used by the throttling system.
/// Currently a unit type since we only care about token availability, not token content.
type Token = ();

pin_project_lite::pin_project! {
    #[project = RateLimiterInnerProj]

    /// Internal enum representing the two throttling modes:
    /// - `Unlimited`: No rate limiting applied
    /// - `Limited`: Rate limited using a token bucket stream
    enum RateLimiterInner {
        /// No rate limiting - operations proceed without delay
        Unlimited,
        /// Rate limited using a token bucket stream
        Limited{#[pin] stream:RateLimitedStream<Iter<Repeat<()>>, AtomicStorage, TokioClock>},
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
    pub(crate) struct Throttler {
        #[pin]
        inner: RateLimiterInner,
        token: Option<Token>,
    }
}

impl Throttler {
    /// Creates a throttler with rate limiting enabled.
    ///
    /// # Arguments
    /// * `bucket` - The token bucket that defines the rate and burst limits
    ///
    /// # Returns
    /// A new throttler that will enforce the bucket's rate limits
    pub fn limited(bucket: TokenBucket<AtomicStorage, TokioClock>) -> Self {
        // Try to consume one token immediately if available
        let token = bucket.consume_one().map(|_| ());
        // Create a rate-limited stream that produces tokens at the bucket's rate
        let stream = futures::stream::iter(iter::repeat(())).rate_limit(bucket);

        Self {
            inner: RateLimiterInner::Limited { stream },
            token,
        }
    }

    /// Creates a throttler with no rate limiting.
    ///
    /// # Returns
    /// A new throttler that allows unlimited operations
    pub fn unlimited() -> Self {
        Self {
            inner: RateLimiterInner::Unlimited,
            token: Some(()), // Always has a token available
        }
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
        let this = self.as_mut().project();
        match this.inner.project() {
            RateLimiterInnerProj::Limited { ref mut stream } => {
                // Remove current token
                this.token.take().expect("token was not available");
                // Try to get next token immediately (non-blocking)
                *this.token = stream.next().now_or_never().flatten();
            }
            RateLimiterInnerProj::Unlimited => {
                // No-op for unlimited throttlers
            }
        }
    }

    /// Checks if the throttler is ready to allow an operation.
    ///
    /// # Returns
    /// `true` if an operation can proceed, `false` if it should wait
    pub fn is_ready(&self) -> bool {
        match self.inner {
            RateLimiterInner::Limited { .. } => self.token.is_some(),
            RateLimiterInner::Unlimited => true, // Always ready
        }
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
        let this = self.project();
        match this.inner.project() {
            RateLimiterInnerProj::Limited { ref mut stream } => {
                // If no token is available, wait for the next one from the stream
                if this.token.is_none() {
                    *this.token = stream.next().await;
                }
            }
            RateLimiterInnerProj::Unlimited => {
                // No-op for unlimited throttlers
            }
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{num::NonZeroU32, time::Duration};
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_unlimited_throttler() {
        let throttler = Throttler::unlimited();
        assert!(throttler.is_ready());

        let mut pinned = std::pin::pin!(throttler);
        pinned.as_mut().consume();
        assert!(pinned.is_ready());

        let result = pinned.as_mut().next().await;
        assert!(result);
        assert!(pinned.is_ready());
    }

    #[tokio::test]
    async fn test_limited_throttler() {
        let bucket = TokenBucket::from_parts(
            gardal::RateLimit::per_second_and_burst(
                NonZeroU32::new(10).unwrap(),
                NonZeroU32::new(5).unwrap(),
            ), // 10 per second, burst of 5
            gardal::TokioClock::default(),
        );
        bucket.add_tokens(5); // Fill the bucket

        let throttler = Throttler::limited(bucket);
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
        let bucket = TokenBucket::from_parts(
            gardal::RateLimit::per_second_and_burst(
                NonZeroU32::new(1).unwrap(),
                NonZeroU32::new(1).unwrap(),
            ), // 1 per second, burst of 1
            gardal::TokioClock::default(),
        );
        bucket.add_tokens(1);

        let throttler = Throttler::limited(bucket);
        assert!(throttler.is_ready());

        let mut pinned = std::pin::pin!(throttler);
        pinned.as_mut().consume();

        // Should not be ready immediately after consumption
        assert!(!pinned.is_ready());

        // Wait for token replenishment
        sleep(Duration::from_millis(1100)).await;

        // Drive the stream forward to get the new token
        pinned.as_mut().next().await;
        assert!(pinned.is_ready());
    }
}
