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

/// The state of a token bucket.
#[derive(Debug, Eq, PartialEq)]
enum TokenStateInner {
    Empty,
    Waiting,
    Ready,
}

pin_project! {

    /// A token bucket state that implements `Future` for throttling operations.
    ///
    /// This struct manages the state of a token bucket and must be driven by polling
    /// the future. The future returns `Some(())` only when the state of the bucket
    /// has changed (e.g., from waiting to ready, or from empty to ready).
    ///
    /// # Behavior
    ///
    /// - **Empty**: Attempts to consume a token immediately. If successful, transitions to Ready.
    ///   If the bucket is empty, schedules a wait and transitions to Waiting.
    /// - **Waiting**: Waits for the scheduled time to elapse, then transitions to Ready.
    /// - **Ready**: Indicates a token is available for consumption.
    ///
    /// # Polling
    ///
    /// The state must be actively polled to make progress. The future will return:
    /// - `Poll::Ready(Some(()))` when the state changes (e.g., Empty→Ready, Waiting→Ready)
    /// - `Poll::Ready(None)` when already in Ready state
    /// - `Poll::Pending` when waiting for time to elapse
    ///
    /// # Usage
    ///
    /// Use `is_ready()` to check if a token is available, and `consume()` to consume it.
    /// Always poll the future to ensure the state machine progresses.
    pub struct TokenState {
        bucket: super::TokenBucket,
        state: TokenStateInner,
        sleep: Option<Pin<Box<tokio::time::Sleep>>>,
    }
}

impl TokenState {
    pub fn new(bucket: super::TokenBucket) -> Self {
        Self {
            bucket,
            state: TokenStateInner::Empty,
            sleep: None,
        }
    }

    /// Returns true if the token state is ready to be consumed.
    pub fn is_ready(&self) -> bool {
        self.state == TokenStateInner::Ready
    }

    /// Consumes the token state.
    ///
    /// # Panics
    ///
    /// Panics if the token state is not ready.
    pub fn consume(&mut self) {
        assert!(self.state == TokenStateInner::Ready);
        self.state = TokenStateInner::Empty;
    }
}

impl Future for TokenState {
    type Output = Option<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        let mut sleep = match this.state {
            TokenStateInner::Empty => {
                match this
                    .bucket
                    .consume_with_borrow(NonZeroU32::new(1).unwrap())
                    .expect("1 is less than burst capacity")
                {
                    Some(wait_time) => Box::pin(tokio::time::sleep(wait_time.into())),
                    None => {
                        *this.state = TokenStateInner::Ready;
                        return Poll::Ready(Some(()));
                    }
                }
            }
            TokenStateInner::Ready => {
                return Poll::Ready(None);
            }
            TokenStateInner::Waiting => this.sleep.take().expect("is set"),
        };

        *this.state = TokenStateInner::Waiting;

        if let Poll::Ready(()) = sleep.as_mut().poll(cx) {
            *this.state = TokenStateInner::Ready;
            return Poll::Ready(Some(()));
        }

        *this.sleep = Some(sleep);
        Poll::Pending
    }
}
