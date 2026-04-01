// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A pool of HTTP/2 connections to a single HTTP authority (scheme + host + port).
//!
//! [`AuthorityPool`] manages multiple [`Connection`] instances, creating new ones
//! on demand when existing connections are fully utilized, and evicting connections
//! that have failed.

use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use http::Uri;
use http_body::Body;
use parking_lot::Mutex;
use tokio::io::{AsyncRead, AsyncWrite};
use tower::Service;

use crate::pool::PoolConfig;
use crate::pool::conn::ConnectionConfigBuilder;

use super::conn::{Connection, ConnectionError, ResponseFuture};

/// Shared mutable state for the pool.
struct AuthorityPoolInner<C> {
    connections: Vec<Connection<C>>,
}

/// A pool of HTTP/2 connections to a single HTTP authority.
///
/// Manages multiple [`Connection`] instances, creating new ones on demand when
/// all existing connections are fully utilized (no available H2 streams), and
/// evicting connections that have entered a closed/failed state.
///
/// Cloning an `AuthorityPool` shares the underlying connection set; each clone
/// maintains its own per-handle state for the `poll_ready`/`call` cycle.
pub struct AuthorityPool<C> {
    connector: C,
    config: PoolConfig,
    inner: Arc<Mutex<AuthorityPoolInner<C>>>,
    /// The readied connection (permit acquired). Consumed by [`call`].
    ready: Option<Connection<C>>,
    /// Connections being polled for readiness. When all connections are at
    /// capacity, we poll all of them so we're woken no matter which one
    /// frees up a stream.
    candidates: Vec<Connection<C>>,
}

impl<C: Clone> Clone for AuthorityPool<C> {
    fn clone(&self) -> Self {
        Self {
            connector: self.connector.clone(),
            config: self.config.clone(),
            inner: Arc::clone(&self.inner),
            ready: None,
            candidates: Vec::new(),
        }
    }
}

impl<C> AuthorityPool<C>
where
    C: Service<Uri> + Clone,
    C::Response: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
    C::Future: Send + 'static,
    C::Error: Into<ConnectionError>,
{
    pub fn new(connector: C, config: PoolConfig) -> Self {
        Self {
            connector,
            config,
            inner: Arc::new(Mutex::new(AuthorityPoolInner {
                connections: Vec::new(),
            })),
            ready: None,
            candidates: Vec::new(),
        }
    }

    /// Polls the pool for a connection with an available H2 stream permit.
    ///
    /// Returns `Ready(Ok(()))` once a permit has been acquired on one connection,
    /// after which [`call`](Self::call) may be invoked exactly once.
    ///
    /// The selection strategy is:
    /// 1. If a permit was already acquired in a previous poll, return immediately.
    /// 2. Poll existing candidate connections (those with available streams).
    ///    The first to become ready wins; the rest are discarded.
    /// 3. If no candidate has capacity, create a new connection (up to
    ///    [`max_connections`](PoolConfig::max_connections)) and poll it.
    /// 4. If already at the connection limit, poll **all** live connections so
    ///    that wakers are registered and we're woken when any stream frees up.
    ///
    /// Closed connections are evicted during each pass. If every candidate
    /// errors out with no pending ones remaining, the last error is returned.
    pub fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectionError>> {
        if self.ready.is_some() {
            return Poll::Ready(Ok(()));
        }

        loop {
            let mut failure = None;
            let mut i = 0;

            while i < self.candidates.len() {
                let candidate = &mut self.candidates[i];
                if candidate.is_closed() {
                    self.candidates.swap_remove(i);
                    continue;
                }

                match candidate.poll_ready(cx) {
                    Poll::Ready(Ok(_)) => {
                        self.ready = Some(self.candidates.swap_remove(i));
                        self.candidates.clear();
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Ready(Err(err)) => {
                        // record the fact we failed on one connection
                        // but we keep trying other candidates! If all
                        // candidates failed we try with a fresh set of candidates
                        self.candidates.swap_remove(i);
                        failure = Some(err);
                    }
                    Poll::Pending => {
                        // We will try the next candidate!
                        //
                        // Waker registered inside conn.poll_ready — we'll be
                        // woken when this connection's h2/permits frees up.
                        i += 1;
                    }
                }
            }

            if let Some(err) = failure
                && self.candidates.is_empty()
            {
                // no more candidates to check.
                // delegate the error back to the caller
                // they can decide to retry
                return Poll::Ready(Err(err));
            }

            if !self.candidates.is_empty() {
                return Poll::Pending;
            }

            // extend the candidates from the current set of connections.
            let mut inner = self.inner.lock();

            // evict closed connections and collect candidates with available streams
            let mut index = 0usize;
            while !inner.connections.is_empty() && index < inner.connections.len() {
                let candidate = &mut inner.connections[index];
                if candidate.is_closed() {
                    inner.connections.swap_remove(index);
                    continue;
                }

                index += 1;
                if candidate.available_streams() == 0 {
                    continue;
                }
                self.candidates.push(candidate.clone());
            }

            if !self.candidates.is_empty() {
                continue;
            }

            // No connection with available capacity. Create a new one if under limit.
            if inner.connections.len() < self.config.max_connections.get() {
                let mut candidate = Connection::new(
                    self.connector.clone(),
                    ConnectionConfigBuilder::default()
                        .initial_max_send_streams(self.config.initial_max_send_streams.get())
                        .keep_alive_interval(self.config.keep_alive_interval)
                        .keep_alive_timeout(self.config.keep_alive_timeout)
                        .build()
                        .unwrap(),
                );
                inner.connections.push(candidate.clone());
                drop(inner);

                match candidate.poll_ready(cx) {
                    Poll::Pending => {
                        self.candidates.push(candidate);
                        continue;
                    }
                    Poll::Ready(Ok(_)) => {
                        self.ready = Some(candidate);
                        self.candidates.clear();
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                }
            }

            // At max connections and all at capacity — poll all connections
            // to register wakers so we're woken when any stream frees up.
            for conn in &inner.connections {
                self.candidates.push(conn.clone());
            }
            drop(inner);
        }
    }

    /// Sends a request over a connection selected by [`poll_ready`].
    ///
    /// # Panics
    /// Panics if called without a prior successful [`poll_ready`].
    pub fn call<B>(&mut self, request: http::Request<B>) -> ResponseFuture<B>
    where
        B: Body<Data = Bytes> + Send + Sync + 'static,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let conn = self
            .ready
            .as_mut()
            .expect("call() invoked without prior poll_ready()");
        let fut = conn.request(request);
        self.ready = None;
        fut
    }
}

#[cfg(test)]
mod test {
    use std::num::{NonZeroU32, NonZeroUsize};
    use std::task::Poll;

    use bytes::Bytes;
    use http::Request;
    use http_body_util::BodyExt;

    use crate::pool::conn::PermittedRecvStream;
    use crate::pool::test_util::TestConnector;

    use super::{AuthorityPool, PoolConfig};

    fn make_pool(
        max_concurrent_streams: u32,
        max_connections: usize,
    ) -> AuthorityPool<TestConnector> {
        AuthorityPool::new(
            TestConnector::new(max_concurrent_streams),
            PoolConfig {
                max_connections: NonZeroUsize::new(max_connections).unwrap(),
                initial_max_send_streams: NonZeroU32::new(max_concurrent_streams).unwrap(),
                ..Default::default()
            },
        )
    }

    async fn send_empty_request(pool: &mut AuthorityPool<TestConnector>) -> PermittedRecvStream {
        futures::future::poll_fn(|cx| pool.poll_ready(cx))
            .await
            .unwrap();
        let resp = pool
            .call(
                Request::builder()
                    .uri("http://test-host:80")
                    .body(http_body_util::Empty::<Bytes>::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        resp.into_body()
    }

    /// First request creates a connection; the pool starts empty.
    #[tokio::test]
    async fn creates_connection_on_demand() {
        let mut pool = make_pool(10, 4);

        {
            let inner = pool.inner.lock();
            assert_eq!(inner.connections.len(), 0);
        }

        let body = send_empty_request(&mut pool).await;

        {
            let inner = pool.inner.lock();
            assert_eq!(inner.connections.len(), 1);
        }

        drop(body);
    }

    /// When all streams on existing connections are busy, a new connection is
    /// created (up to max_connections).
    #[tokio::test]
    async fn scales_up_when_streams_exhausted() {
        // 1 stream per connection, max 3 connections.
        let mut pool = make_pool(1, 3);

        // Hold response bodies to keep streams occupied.
        let b1 = send_empty_request(&mut pool).await;

        // Second request should trigger a second connection.
        let b2 = send_empty_request(&mut pool).await;

        {
            let inner = pool.inner.lock();
            assert_eq!(inner.connections.len(), 2);
        }

        // Third request -> third connection.
        let b3 = send_empty_request(&mut pool).await;

        {
            let inner = pool.inner.lock();
            assert_eq!(inner.connections.len(), 3);
        }

        drop((b1, b2, b3));
    }

    /// The pool does not create more connections than max_connections.
    /// When at capacity and all streams busy, poll_ready returns Pending.
    /// Dropping a held response body frees a stream and unblocks poll_ready.
    #[tokio::test]
    async fn respects_max_connections() {
        // 1 stream per connection, max 2 connections.
        let mut pool = make_pool(1, 2);

        let b1 = send_empty_request(&mut pool).await;
        let b2 = send_empty_request(&mut pool).await;

        {
            let inner = pool.inner.lock();
            assert_eq!(inner.connections.len(), 2);
        }

        // Third poll_ready should return Pending (no capacity).
        let mut pool_clone = pool.clone();
        let result = futures::future::poll_fn(|cx| match pool_clone.poll_ready(cx) {
            Poll::Ready(r) => Poll::Ready(Some(r)),
            Poll::Pending => Poll::Ready(None),
        })
        .await;
        assert!(result.is_none(), "expected Pending when at max capacity");

        // Drop one body, freeing a stream.
        drop(b1);

        // Now poll_ready should succeed (wakers were registered on all connections).
        futures::future::poll_fn(|cx| pool_clone.poll_ready(cx))
            .await
            .unwrap();

        drop(b2);
    }

    /// Cloned pools share the same connection set.
    #[tokio::test]
    async fn clones_share_connections() {
        let pool = make_pool(10, 4);
        let mut p1 = pool.clone();
        let mut p2 = pool.clone();

        let _b1 = send_empty_request(&mut p1).await;

        // p2 should see the connection created by p1.
        {
            let inner = p2.inner.lock();
            assert_eq!(inner.connections.len(), 1);
        }

        let _b2 = send_empty_request(&mut p2).await;

        // Still 1 connection (10 streams available, only 2 used).
        {
            let inner = p1.inner.lock();
            assert_eq!(inner.connections.len(), 1);
        }
    }

    /// Concurrent requests with body echo work correctly through the pool.
    #[tokio::test]
    async fn concurrent_requests_with_echo() {
        let pool = make_pool(10, 4);
        let mut handles = tokio::task::JoinSet::default();

        for i in 0u8..5 {
            let mut p = pool.clone();
            handles.spawn(async move {
                futures::future::poll_fn(|cx| p.poll_ready(cx))
                    .await
                    .unwrap();
                let resp = p
                    .call(
                        Request::builder()
                            .uri("http://test-host:80")
                            .body(http_body_util::Full::new(Bytes::from(vec![i; 4])))
                            .unwrap(),
                    )
                    .await
                    .unwrap();

                let collected = resp.into_body().collect().await.unwrap().to_bytes();
                assert_eq!(
                    collected.as_ref(),
                    &[i; 4],
                    "response should echo request body"
                );
            });
        }

        handles.join_all().await;
    }
}
