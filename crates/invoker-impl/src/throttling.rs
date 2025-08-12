// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    num::NonZeroU32,
    pin::Pin,
    task::{Context, Poll},
};

use pin_project_lite::pin_project;

/// The internal state of a token bucket throttling mechanism.
#[derive(Debug)]
enum TokenStateInner {
    /// Waiting for the next token to become available after a specified duration.
    Waiting(Pin<Box<tokio::time::Sleep>>),
    /// Ready to consume a token immediately.
    Ready,
}

pin_project! {
    /// A token bucket throttling state that implements `Future` for asynchronous rate limiting.
    ///
    /// This struct manages the throttling state of a token bucket and must be actively polled
    /// to make progress. The future returns `Some(())` when the state transitions (e.g., from
    /// waiting to ready), and `None` when already in the ready state.
    ///
    /// # State Machine Behavior
    ///
    /// The implementation uses a state machine with two main states:
    /// - **Ready**: A token is immediately available for consumption
    /// - **Waiting**: Waiting for the scheduled time to elapse before a token becomes available
    ///
    /// # Polling Behavior
    ///
    /// The future must be actively polled to drive state transitions:
    /// - `Poll::Ready(Some(()))` when transitioning from Waiting to Ready
    /// - `Poll::Ready(None)` when already in Ready state
    /// - `Poll::Pending` when waiting for time to elapse
    ///
    /// # Usage Pattern
    ///
    /// 1. Check if ready using `is_ready()`
    /// 2. If ready, consume the token with `consume()`
    /// 3. Always poll the future to ensure state transitions occur
    /// 4. The `next()` method automatically manages state transitions based on token availability
    pub struct TokenState {
        bucket: super::TokenBucket,
        state: TokenStateInner,
    }
}

impl TokenState {
    /// Creates a new `TokenState` with the given token bucket.
    ///
    /// The initial state is determined by calling `next()` to check if tokens are immediately
    /// available or if waiting is required.
    pub fn new(bucket: super::TokenBucket) -> Self {
        let mut this = Self {
            bucket,
            state: TokenStateInner::Ready,
        };
        this.next();
        this
    }

    /// Returns `true` if the token state is ready for immediate consumption.
    ///
    /// This method checks the current state without consuming any tokens.
    pub fn is_ready(&self) -> bool {
        matches!(self.state, TokenStateInner::Ready)
    }

    /// Consumes the current token and advances to the next state.
    ///
    /// This method will panic if called when the state is not ready. Always check
    /// `is_ready()` before calling this method.
    ///
    /// # Panics
    ///
    /// Panics if the token state is not ready (i.e., if `is_ready()` returns `false`).
    pub fn consume(&mut self) {
        assert!(
            matches!(self.state, TokenStateInner::Ready),
            "token state is not ready"
        );

        self.next();
    }

    /// Advances the token state based on current bucket availability.
    ///
    /// This method attempts to consume a token immediately. If successful, the state
    /// transitions to Ready. If the bucket is empty, it schedules a wait and transitions
    /// to Waiting state.
    fn next(&mut self) {
        // Try to drive the state in case we can move immediately to ready state. Otherwise go into waiting state.
        match self
            .bucket
            .consume_with_borrow(unsafe { NonZeroU32::new_unchecked(1) })
            .expect("1 is less than burst capacity")
        {
            None => self.state = TokenStateInner::Ready,
            Some(wait_time) => {
                self.state =
                    TokenStateInner::Waiting(Box::pin(tokio::time::sleep(wait_time.into())));
            }
        }
    }
}

impl Future for TokenState {
    type Output = Option<()>;

    /// Polls the token state to drive state transitions.
    ///
    /// This implementation handles the async state machine:
    /// - If in Waiting state, checks if the sleep timer has completed and transitions to Ready
    /// - If in Ready state, returns `None` to indicate no state change
    /// - Returns `Pending` while waiting for time to elapse
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        match this.state {
            TokenStateInner::Waiting(sleep) => {
                if let Poll::Ready(()) = sleep.as_mut().poll(cx) {
                    *this.state = TokenStateInner::Ready;
                    return Poll::Ready(Some(()));
                }
            }
            TokenStateInner::Ready => {
                return Poll::Ready(None);
            }
        };

        Poll::Pending
    }
}

// #[cfg(test)]
// mod tests {
//     use std::num::NonZeroU32;
//     use std::time::Duration;

//     use futures::FutureExt;
//     use googletest::assert_that;
//     use googletest::prelude::*;
//     use tokio::time::sleep;

//     use super::*;

//     fn create_token_bucket_with_tokens(tokens: u32) -> super::super::TokenBucket {
//         use gardal::{RateLimit, TokenBucket as GardalTokenBucket};

//         let clock = gardal::ManualClock::new(0);
//         let bucket = GardalTokenBucket::from_parts(
//             RateLimit::per_second(unsafe { NonZeroU32::new_unchecked(tokens) }),
//             clock,
//         );
//         bucket.add_tokens(tokens);
//         bucket
//     }

//     fn create_empty_token_bucket() -> super::super::TokenBucket {
//         // since there is no way to create an empty token bucket, we create a bucket with a rate limit of 1 per hour and a burst of 1
//         use gardal::{RateLimit, TokenBucket as GardalTokenBucket};

//         GardalTokenBucket::from_parts(
//             RateLimit::per_hour(unsafe { NonZeroU32::new_unchecked(1) })
//                 .with_burst(unsafe { NonZeroU32::new_unchecked(1) }),
//             gardal::TokioClock::default(),
//         )
//     }

//     #[tokio::test]
//     async fn test_token_state_new() {
//         let bucket = create_token_bucket_with_tokens(10);
//         let token_state = TokenState::new(bucket);

//         assert!(!token_state.is_ready());
//     }

//     #[tokio::test]
//     async fn test_token_state_immediate_ready() {
//         let bucket = create_token_bucket_with_tokens(10);
//         let mut token_state = TokenState::new(bucket);
//         // First poll should immediately transition to Ready since tokens are available
//         let result = token_state.poll_unpin(&mut std::task::Context::from_waker(
//             futures::task::noop_waker_ref(),
//         ));

//         assert_that!(result, eq(Poll::Ready(Some(()))));
//         assert!(token_state.is_ready());
//     }

//     #[tokio::test]
//     async fn test_token_state_empty_to_waiting() {
//         let bucket = create_empty_token_bucket();
//         let mut token_state = TokenState::new(bucket);

//         // First poll should transition to Waiting since no tokens are available
//         let result = token_state.poll_unpin(&mut std::task::Context::from_waker(
//             futures::task::noop_waker_ref(),
//         ));

//         assert_that!(result, eq(Poll::Pending));
//     }

//     #[tokio::test]
//     async fn test_token_state_ready_returns_none() {
//         let bucket = create_token_bucket_with_tokens(10);
//         let mut token_state = TokenState::new(bucket);

//         // First poll to get to Ready state
//         let _ = token_state.poll_unpin(&mut std::task::Context::from_waker(
//             futures::task::noop_waker_ref(),
//         ));

//         // Second poll should return None since already in Ready state
//         let result = token_state.poll_unpin(&mut std::task::Context::from_waker(
//             futures::task::noop_waker_ref(),
//         ));

//         assert_that!(result, eq(Poll::Ready(None)));
//         assert!(token_state.is_ready());
//     }

//     #[tokio::test]
//     async fn test_token_state_consume_ready() {
//         let bucket = create_token_bucket_with_tokens(10);
//         let mut token_state = TokenState::new(bucket);

//         // Get to Ready state
//         let _ = token_state.poll_unpin(&mut std::task::Context::from_waker(
//             futures::task::noop_waker_ref(),
//         ));

//         assert!(token_state.is_ready());
//         token_state.consume();

//         // Since we have 10 tokens, we should be ready again immediately
//         assert!(token_state.is_ready());
//     }

//     #[tokio::test]
//     async fn test_token_state_consume_ready_then_wait() {
//         let bucket = create_empty_token_bucket();
//         bucket.add_tokens(1);
//         let mut token_state = TokenState::new(bucket);

//         // Get to Ready state
//         let _ = token_state.poll_unpin(&mut std::task::Context::from_waker(
//             futures::task::noop_waker_ref(),
//         ));

//         assert!(token_state.is_ready());
//         token_state.consume();

//         // Since we don't have any tokens, we should be waiting
//         assert!(!token_state.is_ready());
//     }

//     #[tokio::test]
//     #[should_panic(expected = "token state is not ready")]
//     async fn test_token_state_consume_not_ready_panics() {
//         let bucket = create_empty_token_bucket();
//         let mut token_state = TokenState::new(bucket);

//         // Try to consume without being ready - should panic
//         token_state.consume();
//     }

//     #[tokio::test]
//     async fn test_token_state_waiting_to_ready() {
//         let bucket = create_token_bucket_with_tokens(1);
//         let mut token_state = TokenState::new(bucket);

//         // consume the first token

//         // First poll to get to Waiting state
//         let _ = token_state.poll_unpin(&mut std::task::Context::from_waker(
//             futures::task::noop_waker_ref(),
//         ));

//         // State should be Waiting when sleep is set
//         assert!(token_state.sleep.is_some());

//         // Wait for the sleep to complete (this is a very short duration in our test bucket)
//         sleep(Duration::from_millis(10)).await;

//         // Poll again - should transition to Ready
//         let result = token_state.poll_unpin(&mut std::task::Context::from_waker(
//             futures::task::noop_waker_ref(),
//         ));

//         match result {
//             std::task::Poll::Ready(Some(())) => {
//                 assert!(token_state.is_ready());
//                 // State should be Ready when is_ready() returns true
//             }
//             std::task::Poll::Pending => {
//                 // If still pending, wait a bit more and try again
//                 sleep(Duration::from_millis(10)).await;
//                 let result2 = token_state.poll_unpin(&mut std::task::Context::from_waker(
//                     futures::task::noop_waker_ref(),
//                 ));
//                 match result2 {
//                     std::task::Poll::Ready(Some(())) => {
//                         assert!(token_state.is_ready());
//                         // State should be Ready when is_ready() returns true
//                     }
//                     _ => panic!("Expected Ready(Some(())) after waiting"),
//                 }
//             }
//             _ => panic!("Expected Ready(Some(())) or Pending after waiting"),
//         }
//     }

//     #[tokio::test]
//     async fn test_token_state_multiple_consumes() {
//         let bucket = create_token_bucket_with_tokens(5);
//         let mut token_state = TokenState::new(bucket);

//         // Consume tokens multiple times
//         for i in 0..5 {
//             // Poll to get to Ready state
//             let _ = token_state.poll_unpin(&mut std::task::Context::from_waker(
//                 futures::task::noop_waker_ref(),
//             ));

//             assert!(
//                 token_state.is_ready(),
//                 "Should be ready for consumption {}",
//                 i
//             );
//             token_state.consume();
//         }

//         // After consuming all tokens, next poll should go to Waiting
//         let result = token_state.poll_unpin(&mut std::task::Context::from_waker(
//             futures::task::noop_waker_ref(),
//         ));

//         assert_that!(result, eq(Poll::Pending));
//     }

//     #[tokio::test]
//     async fn test_token_state_is_ready() {
//         let bucket = create_token_bucket_with_tokens(1);
//         let mut token_state = TokenState::new(bucket);

//         // Initially not ready
//         assert!(!token_state.is_ready());

//         // Poll to get to Ready state
//         let _ = token_state.poll_unpin(&mut std::task::Context::from_waker(
//             futures::task::noop_waker_ref(),
//         ));

//         // Now should be ready
//         assert!(token_state.is_ready());

//         // Consume the token
//         token_state.consume();

//         // Should still be ready if more tokens available, or not ready if waiting
//         // We can test this by checking if is_ready() returns true or if sleep is set
//         if token_state.is_ready() {
//             // If ready, sleep should not be set
//             assert!(token_state.sleep.is_none());
//         } else {
//             // If not ready, sleep should be set (indicating Waiting state)
//             assert!(token_state.sleep.is_some());
//         }
//     }
// }
