// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::{io, mem};

use bytes::Bytes;
use futures::channel::oneshot;
use futures::future::{BoxFuture, poll_fn};
use futures::{FutureExt, ready};
use h2::client::{ResponseFuture, SendRequest};
use h2::{Reason, RecvStream, SendStream};
use http::{HeaderMap, Request, Response, Uri};
use http_body::{Body, Frame};
use http_body_util::BodyExt;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{AcquireError, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use tower::Service;
use tracing::debug;

/// Errors that can occur during the lifecycle of an H2 connection.
#[derive(thiserror::Error, Debug)]
pub enum ConnectionError {
    #[error(transparent)]
    IO(#[from] io::Error),
    #[error(transparent)]
    H2(#[from] h2::Error),

    #[error("connection has been cancelled during initialization")]
    Cancelled,

    #[error("connection is closed")]
    Closed,
}

type BoxSyncFuture = Pin<
    Box<dyn Future<Output = Result<OwnedSemaphorePermit, AcquireError>> + Send + Sync + 'static>,
>;
/// A lazily-initialized, multiplexed HTTP/2 connection.
///
/// `Connection` wraps a connector `C` (a Tower [`Service`] that produces an async I/O stream)
/// and lazily performs the H2 handshake on the first request. Subsequent requests reuse the
/// same underlying H2 connection.
///
/// Concurrency is bounded by a semaphore that limits the number of in-flight H2 streams
/// (configured via `init_max_streams`, which sets both the semaphore and
/// `h2::client::Builder::initial_max_send_streams`). Callers must call
/// [`poll_ready`](Self::poll_ready) (or [`ready`](Self::ready)) before each
/// [`request`](Self::request) to acquire a stream permit.
///
/// Cloning a `Connection` shares the underlying H2 session; the clone starts without a
/// permit or in-progress acquire future.
pub struct Connection<C> {
    connector: C,
    /// Tracks the current semaphore size and updates it
    /// based on the last known max_concurrent_streams
    semaphore_updater: Arc<SemaphoreUpdater>,
    /// Bounds the number of concurrent H2 streams on this connection.
    semaphore: Arc<Semaphore>,
    /// Shared connection state. The mutex is held only briefly for state transitions.
    inner: Arc<Mutex<ConnectionInner>>,
    /// Permit acquired via [`poll_ready`](Self::poll_ready), consumed by [`request`](Self::request).
    permit: Option<OwnedSemaphorePermit>,
    /// In-progress semaphore acquire, if any.
    acquire: Option<BoxSyncFuture>,
}

impl<C> Clone for Connection<C>
where
    C: Clone,
{
    fn clone(&self) -> Self {
        Self {
            connector: self.connector.clone(),
            semaphore_updater: Arc::clone(&self.semaphore_updater),
            semaphore: Arc::clone(&self.semaphore),
            inner: Arc::clone(&self.inner),
            permit: None,
            acquire: None,
        }
    }
}

impl<C> Connection<C>
where
    C: Service<Uri>,
    C::Response: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
    C::Future: Send + 'static,
    C::Error: Into<ConnectionError>,
{
    pub fn new(connector: C, init_max_streams: u32) -> Self {
        Self {
            connector,
            semaphore_updater: Arc::new(SemaphoreUpdater::new(init_max_streams as usize)),
            semaphore: Arc::new(Semaphore::new(init_max_streams as usize)),
            inner: Arc::new(Mutex::new(ConnectionInner::New)),
            permit: None,
            acquire: None,
        }
    }

    pub async fn ready(&mut self) -> Result<(), ConnectionError> {
        poll_fn(|cx| self.poll_ready(cx)).await
    }

    #[cfg(test)]
    pub fn try_ready(&mut self) -> Option<Result<(), ConnectionError>> {
        use std::task::Waker;
        match self.poll_ready(&mut Context::from_waker(Waker::noop())) {
            Poll::Pending => {
                // drop the acquire future since it will never be polled again
                // to clear up Semaphore resources
                self.acquire = None;
                self.permit = None;
                None
            }
            Poll::Ready(result) => Some(result),
        }
    }

    /// Return the number of the available streams on this connection.
    ///
    /// This does not guarantee that poll_ready(), try_ready(), or ready()
    /// will succeed. It can only be used to get an estimate of how many
    /// h2 streams are available
    pub fn available_streams(&self) -> usize {
        self.semaphore.available_permits()
    }

    pub fn max_concurrent_streams(&self) -> usize {
        self.semaphore_updater.current()
    }

    /// Returns `true` if the connection has been closed or encountered a fatal error.
    pub fn is_closed(&self) -> bool {
        matches!(&*self.inner.lock().unwrap(), ConnectionInner::Closed)
    }

    /// Must be polled before each request. This makes sure we acquire the permit
    /// to open a new h2 stream.
    /// This should return immediately if connection has enough permits. Otherwise
    /// it will return Pending.
    ///
    /// If you want to wait on the connection to be ready, use `ready()` instead.
    pub fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectionError>> {
        match self.inner.lock().unwrap().deref_mut() {
            ConnectionInner::New => {
                // Note: the connector is always ready.
                ready!(self.connector.poll_ready(cx)).map_err(Into::into)?;
            }
            ConnectionInner::Closed => {
                return Poll::Ready(Err(ConnectionError::Closed));
            }
            ConnectionInner::Connected {
                send_request,
                cancel,
            } => {
                if cancel.is_cancelled() {
                    return Poll::Ready(Err(ConnectionError::Closed));
                }

                ready!(send_request.poll_ready(cx))?;
                // this is a good synchronization point to update the permits
                // to the last known size known by the send_request object.
                self.semaphore_updater
                    .update(send_request.current_max_send_streams(), &self.semaphore);
            }
            ConnectionInner::Connecting { .. } => {}
        }

        if self.permit.is_some() {
            return Poll::Ready(Ok(()));
        }

        if self.acquire.is_none() {
            self.acquire = Some(Box::pin(self.semaphore.clone().acquire_owned()));
        }

        let acquire = self.acquire.as_mut().unwrap();
        // The semaphore can never be closed because we already own it
        self.permit = Some(ready!(acquire.poll_unpin(cx)).unwrap());
        self.acquire = None;

        Poll::Ready(Ok(()))
    }

    /// Sends an HTTP request over the shared H2 connection.
    ///
    /// # Panics
    /// Panics if called without a prior successful [`poll_ready`](Self::poll_ready) call.
    pub fn request<B>(&mut self, request: http::Request<B>) -> RequestFuture<B>
    where
        B: Body<Data = Bytes> + Unpin + Send + Sync + 'static,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        assert!(
            self.permit.is_some(),
            "called request() without calling poll_ready()"
        );
        // we already have a permit.
        let permit = self.permit.take().unwrap();

        let mut inner = self.inner.lock().unwrap();

        let state = match &mut *inner {
            ConnectionInner::Closed => RequestFutureState::error(ConnectionError::Closed),
            ConnectionInner::New => {
                let weak_inner = Arc::downgrade(&self.inner);

                let connect = self.connector.call(request.uri().clone());
                let max_streams = self.semaphore_updater.current();
                let state = RequestFutureState::drive(async move {
                    let stream = connect.await.map_err(Into::into)?;

                    let (send_request, connection) = h2::client::Builder::new()
                        .initial_max_send_streams(max_streams)
                        .handshake::<_, Bytes>(stream)
                        .await?;

                    let cancel = CancellationToken::new();
                    let cancellation = cancel.clone();
                    tokio::spawn(async move {
                        tokio::select! {
                            Err(err) = connection => {
                                debug!("h2 connection error: {err}");
                            },
                            _ = cancellation.cancelled() => {
                                // connection has been cancelled
                            }
                        };

                        // set state to closed
                        if let Some(state) = weak_inner.upgrade() {
                            *state.lock().unwrap() = ConnectionInner::Closed;
                        }
                    });

                    Ok((send_request, cancel))
                });

                // by setting the connection state to "connecting"
                // all new requests will land in "waiting"
                *inner = ConnectionInner::Connecting {
                    waiters: Vec::default(),
                };
                state
            }
            ConnectionInner::Connected { send_request, .. } => RequestFutureState::PreFlight {
                send_request: send_request.clone(),
            },
            ConnectionInner::Connecting { waiters } => {
                let (tx, rx) = oneshot::channel();
                // register myself to be notified once connection is ready
                waiters.push(tx);

                RequestFutureState::WaitingConnection { rx }
            }
        };

        RequestFuture {
            permit: Some(permit),
            connection: Arc::clone(&self.inner),
            request: Some(request),
            state,
        }
    }
}

/// State machine for the shared H2 connection.
///
/// Transitions: `New` -> `Connecting` -> `Connected` -> `Closed`.
/// The connection can also move directly from `New` or `Connecting` to `Closed` on error.
enum ConnectionInner {
    /// No connection attempt has been made yet.
    New,
    /// H2 handshake is in progress. Other requests arriving during this phase register
    /// a oneshot sender so they can be notified once the `SendRequest` handle is available.
    Connecting {
        waiters: Vec<oneshot::Sender<SendRequest<Bytes>>>,
    },
    /// Handshake completed; the `SendRequest` handle is ready for use.
    Connected {
        send_request: SendRequest<Bytes>,
        cancel: CancellationToken,
    },
    /// The connection has been closed or encountered a fatal error.
    Closed,
}

/// Internal state machine for a single in-flight request on a [`Connection`].
///
/// Each variant represents a phase of the request lifecycle:
/// - **Driving** – this request is driving the initial H2 handshake.
/// - **WaitingConnection** – another request is driving the handshake; we wait for notification.
/// - **PreFlight** – we have a `SendRequest` handle and are waiting for H2 stream capacity.
/// - **Inflight** – the request has been sent; we are waiting for the response.
/// - **Error** – a terminal error was captured for the caller to consume.
#[pin_project::pin_project(project=RequestFutureStateProject)]
enum RequestFutureState {
    Driving {
        #[pin]
        fut: BoxFuture<'static, Result<(SendRequest<Bytes>, CancellationToken), ConnectionError>>,
    },
    WaitingConnection {
        #[pin]
        rx: oneshot::Receiver<SendRequest<Bytes>>,
    },
    PreFlight {
        send_request: SendRequest<Bytes>,
    },
    Inflight {
        #[pin]
        fut: ResponseFuture,
    },
    Error {
        err: Option<ConnectionError>,
    },
}

impl RequestFutureState {
    fn error(err: impl Into<ConnectionError>) -> Self {
        Self::Error {
            err: Some(err.into()),
        }
    }

    fn drive<F>(fut: F) -> Self
    where
        F: Future<Output = Result<(SendRequest<Bytes>, CancellationToken), ConnectionError>>
            + Send
            + 'static,
    {
        Self::Driving { fut: Box::pin(fut) }
    }
}

/// Future returned by [`Connection::request`].
///
/// Drives the request through its [`RequestFutureState`] state machine until a response
/// is received. Holds a semaphore permit for the duration of the request to bound
/// concurrent H2 streams.
///
/// On drop, if this future was responsible for driving the H2 handshake (i.e. the
/// connection is still in `Connecting` state), the connection is moved to `Closed` to
/// prevent waiters from hanging indefinitely.
#[pin_project::pin_project(PinnedDrop)]
pub struct RequestFuture<B> {
    permit: Option<OwnedSemaphorePermit>,
    connection: Arc<Mutex<ConnectionInner>>,
    request: Option<http::Request<B>>,
    #[pin]
    state: RequestFutureState,
}

#[pin_project::pinned_drop]
impl<B> PinnedDrop for RequestFuture<B> {
    fn drop(self: Pin<&mut Self>) {
        // if the `Connecting`` future was dropped (while in Connecting state). we need to
        // immediately switch connection to closed to make sure
        // waiters are immediately  notified otherwise they will be stuck forever
        let this = self.project();
        if let RequestFutureStateProject::Driving { .. } = this.state.project() {
            let mut state = this.connection.lock().unwrap();
            if let ConnectionInner::Connecting { waiters: _ } = &*state {
                *state = ConnectionInner::Closed;
            }
        }
    }
}

impl<B> Future for RequestFuture<B>
where
    B: Body<Data = Bytes> + Unpin + Send + Sync + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send,
{
    type Output = Result<Response<PermittedRecvStream>, ConnectionError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        loop {
            match this.state.as_mut().project() {
                RequestFutureStateProject::Error { err } => {
                    let mut inner = this.connection.lock().unwrap();
                    // notify the connection routine that in case it did not already exit
                    if let ConnectionInner::Connected { cancel, .. } = &*inner {
                        cancel.cancel();
                    }

                    // we make sure to set the state 'closed'
                    *inner = ConnectionInner::Closed;
                    return Poll::Ready(Err(err.take().expect("future polled after finish")));
                }
                RequestFutureStateProject::Driving { fut } => {
                    let (send_request, cancel) = match ready!(fut.poll(cx)) {
                        Ok(stream) => stream,
                        Err(err) => {
                            this.state.set(RequestFutureState::error(err));
                            continue;
                        }
                    };

                    this.state.set(RequestFutureState::PreFlight {
                        send_request: send_request.clone(),
                    });

                    let mut state = this.connection.lock().unwrap();

                    let state = mem::replace(
                        &mut *state,
                        ConnectionInner::Connected {
                            send_request: send_request.clone(),
                            cancel,
                        },
                    );

                    // we expect state to be in Connecting state.
                    // but it also can be in a cancel state if the connection has exited before
                    // we reach this state.
                    let ConnectionInner::Connecting { waiters } = state else {
                        continue;
                    };

                    for waiter in waiters {
                        _ = waiter.send(send_request.clone());
                    }
                }
                RequestFutureStateProject::WaitingConnection { rx } => {
                    let send_request = match ready!(rx.poll(cx)) {
                        Ok(send_request) => send_request,
                        Err(_) => {
                            this.state
                                .set(RequestFutureState::error(ConnectionError::Cancelled));
                            continue;
                        }
                    };

                    this.state
                        .set(RequestFutureState::PreFlight { send_request });
                }
                RequestFutureStateProject::PreFlight { send_request } => {
                    if let Err(err) = ready!(send_request.poll_ready(cx)) {
                        this.state.set(RequestFutureState::error(err));
                        continue;
                    }

                    // we finally can forward the request now
                    let (parts, body) = this.request.take().unwrap().into_parts();

                    let req = Request::from_parts(parts, ());
                    let end_stream = body.is_end_stream();
                    let (fut, send_stream) = send_request.send_request(req, end_stream)?;

                    if !end_stream {
                        tokio::spawn(RequestPumpTask::new(send_stream, body).run());
                    }
                    this.state.set(RequestFutureState::Inflight { fut });
                }
                RequestFutureStateProject::Inflight { fut } => {
                    let resp = ready!(fut.poll(cx)).map_err(ConnectionError::from)?;
                    let permit = this.permit.take().expect("permit already taken");
                    let resp = resp.map(|recv| PermittedRecvStream::new(recv, permit));
                    return Poll::Ready(Ok(resp));
                }
            }
        }
    }
}

/// Background task that streams the request body into an H2 `SendStream`.
///
/// Spawned by [`RequestFuture`] once the H2 stream is established. Reads frames from
/// the body, respects H2 flow-control by reserving and polling capacity before each
/// write, and sends trailers (or empty trailers) once the body is exhausted.
struct RequestPumpTask<B> {
    send_stream: SendStream<Bytes>,
    body: B,
}

impl<B> RequestPumpTask<B>
where
    B: http_body::Body<Data = Bytes> + Unpin,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send,
{
    fn new(send_stream: SendStream<Bytes>, body: B) -> Self {
        Self { send_stream, body }
    }

    async fn run(mut self) {
        if let Err(err) = self.run_inner().await {
            debug!(%err, "failed while sending request stream");
            self.send_stream.send_reset(Reason::CANCEL);
        }
    }

    async fn run_inner(&mut self) -> Result<(), h2::Error> {
        while let Some(frame) = self.body.frame().await {
            match frame {
                Ok(frame) => {
                    if self.handle_frame(frame, self.body.is_end_stream()).await? {
                        // end stream already sent!
                        return Ok(());
                    }
                }
                Err(err) => {
                    debug!("error while reading request stream: {}", err.into());
                    self.send_stream.send_reset(Reason::CANCEL);
                    return Ok(());
                }
            }
        }

        // Send an explicit end stream
        self.send_stream.send_trailers(HeaderMap::default())?;

        Ok(())
    }

    /// handle a frame, returns true if it's last frame (trailers). It's illegal to
    /// send more data frames after handle_frame returns true
    async fn handle_frame(
        &mut self,
        frame: Frame<Bytes>,
        end_of_stream: bool,
    ) -> Result<bool, h2::Error> {
        if frame.is_data() {
            let mut data = frame.into_data().unwrap();

            let mut end = false;
            while !data.is_empty() {
                self.send_stream.reserve_capacity(data.len());
                let size = poll_fn(|cx| self.send_stream.poll_capacity(cx))
                    .await
                    .ok_or(Reason::FLOW_CONTROL_ERROR)??;

                let chunk = data.split_to(size.min(data.len()));
                end = end_of_stream && data.is_empty();
                self.send_stream.send_data(chunk, end)?;
            }
            Ok(end)
        } else if frame.is_trailers() {
            // trailers!
            let trailers = frame.into_trailers().unwrap();
            self.send_stream.send_trailers(trailers)?;
            Ok(true)
        } else {
            Err(Reason::PROTOCOL_ERROR.into())
        }
    }
}

/// Response body stream that holds an H2 stream permit for its lifetime.
///
/// Implements [`http_body::Body`] by delegating to the inner [`RecvStream`],
/// automatically releasing H2 flow-control capacity after each data frame.
/// The semaphore permit is held until this stream is dropped, ensuring the
/// concurrency slot remains occupied while the response body is being consumed.
#[derive(Debug)]
pub struct PermittedRecvStream {
    stream: RecvStream,
    /// Tracks whether all data frames have been consumed and we should poll trailers next.
    data_done: bool,
    _permit: OwnedSemaphorePermit,
}

impl PermittedRecvStream {
    fn new(stream: RecvStream, permit: OwnedSemaphorePermit) -> Self {
        Self {
            stream,
            data_done: false,
            _permit: permit,
        }
    }
}

impl Body for PermittedRecvStream {
    type Data = Bytes;
    type Error = h2::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        if !self.data_done {
            match ready!(self.stream.poll_data(cx)) {
                Some(Ok(data)) => {
                    let len = data.len();
                    let _ = self.stream.flow_control().release_capacity(len);
                    return Poll::Ready(Some(Ok(Frame::data(data))));
                }
                Some(Err(err)) => return Poll::Ready(Some(Err(err))),
                None => {
                    self.data_done = true;
                }
            }
        }

        // Data is exhausted, poll for trailers
        match ready!(self.stream.poll_trailers(cx)) {
            Ok(Some(trailers)) => Poll::Ready(Some(Ok(Frame::trailers(trailers)))),
            Ok(None) => Poll::Ready(None),
            Err(err) => Poll::Ready(Some(Err(err))),
        }
    }

    fn is_end_stream(&self) -> bool {
        self.stream.is_end_stream()
    }
}

/// A utility to track and update the current size
/// of the connection Semaphore and synchronize
/// it with the latest know value of max_send_streams
struct SemaphoreUpdater {
    current: AtomicUsize,
    target: AtomicUsize,
}

impl SemaphoreUpdater {
    fn new(size: usize) -> Self {
        Self {
            current: AtomicUsize::new(size),
            target: AtomicUsize::new(size),
        }
    }

    fn current(&self) -> usize {
        self.current.load(Ordering::Relaxed)
    }

    fn update(&self, current_max_send_streams: usize, permits: &Semaphore) {
        let current_max_send_streams = current_max_send_streams.min(Semaphore::MAX_PERMITS);
        self.target
            .store(current_max_send_streams, Ordering::Relaxed);
        let mut current = self.current.load(Ordering::Relaxed);
        loop {
            let target = self.target.load(Ordering::Relaxed);
            match target.cmp(&current) {
                std::cmp::Ordering::Equal => {
                    return;
                }
                std::cmp::Ordering::Greater => {
                    match self.current.compare_exchange(
                        current,
                        target,
                        Ordering::Release,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            permits.add_permits(target - current);
                            return;
                        }
                        Err(c) => {
                            current = c;
                        }
                    }
                }
                std::cmp::Ordering::Less => {
                    match self.current.compare_exchange(
                        current,
                        target,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => {
                            let to_remove = current - target;
                            let removed = permits.forget_permits(to_remove);
                            match removed.cmp(&to_remove) {
                                std::cmp::Ordering::Less => {
                                    self.current
                                        .fetch_add(to_remove - removed, Ordering::Relaxed);
                                    return;
                                }
                                std::cmp::Ordering::Greater => {
                                    unreachable!();
                                }
                                std::cmp::Ordering::Equal => {
                                    return;
                                }
                            }
                        }
                        Err(c) => {
                            current = c;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::io;
    use std::task::{Context, Poll};

    use bytes::Bytes;
    use futures::future::BoxFuture;
    use http::{Request, Response, StatusCode, Uri};
    use http_body::Frame;
    use http_body_util::BodyExt;
    use tokio::io::DuplexStream;
    use tokio::sync::mpsc;
    use tokio::task::JoinSet;
    use tower::Service;

    use super::Connection;

    /// In-process h2 server configuration.
    struct ServerConfig {
        max_concurrent_streams: u32,
    }

    /// A test connector that creates in-memory duplex streams and spawns an
    /// h2 server on the other end.
    #[derive(Clone)]
    struct TestConnector {
        config: std::sync::Arc<ServerConfig>,
    }

    impl TestConnector {
        fn new(max_concurrent_streams: u32) -> Self {
            Self {
                config: std::sync::Arc::new(ServerConfig {
                    max_concurrent_streams,
                }),
            }
        }
    }

    impl Service<Uri> for TestConnector {
        type Response = DuplexStream;
        type Error = io::Error;
        type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Uri) -> Self::Future {
            let config = std::sync::Arc::clone(&self.config);
            Box::pin(async move {
                let (client, server) = tokio::io::duplex(64 * 1024);
                tokio::spawn(run_server(server, config));
                Ok(client)
            })
        }
    }

    /// Runs an h2 server on the given stream. For each request, echoes the
    /// request body back in the response and sends empty trailers when done.
    async fn run_server(stream: DuplexStream, config: std::sync::Arc<ServerConfig>) {
        let mut h2 = h2::server::Builder::new()
            .max_concurrent_streams(config.max_concurrent_streams)
            .handshake::<_, Bytes>(stream)
            .await
            .unwrap();

        while let Some(request) = h2.accept().await {
            let (request, mut respond) = request.unwrap();
            tokio::spawn(async move {
                let response = Response::builder().status(StatusCode::OK).body(()).unwrap();
                let mut send_stream = respond.send_response(response, false).unwrap();
                let mut recv_body = request.into_body();

                while let Some(data) = recv_body.data().await {
                    let data = data.unwrap();
                    recv_body
                        .flow_control()
                        .release_capacity(data.len())
                        .unwrap();

                    send_stream.reserve_capacity(data.len());
                    let _ = futures::future::poll_fn(|cx| send_stream.poll_capacity(cx)).await;
                    if send_stream.send_data(data, false).is_err() {
                        return;
                    }
                }

                let _ = send_stream.send_trailers(http::HeaderMap::new());
            });
        }
    }

    /// Sends a request with an empty body and returns the response body stream.
    /// The response body must be consumed or dropped by the caller.
    async fn send_request(conn: &mut Connection<TestConnector>) -> super::PermittedRecvStream {
        conn.ready().await.unwrap();
        let resp = conn
            .request(
                Request::builder()
                    .uri("http://test-host:80")
                    .body(http_body_util::Empty::<Bytes>::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        resp.into_body()
    }

    /// Client starts with init_max_streams=100 but server advertises
    /// max_concurrent_streams=5. After the first request round-trip triggers
    /// the handshake and a subsequent poll_ready reads the updated setting,
    /// the semaphore must shrink to 5.
    #[tokio::test]
    async fn permits_sync_with_server_max_concurrent_streams() {
        let mut connection = Connection::new(TestConnector::new(5), 100);

        // First request triggers the handshake. Drop the body to release permit.
        drop(send_request(&mut connection).await);

        // Second ready() hits the Connected arm which calls semaphore_updater.update(),
        // syncing the semaphore down to 5. Send a request to consume that permit too.
        drop(send_request(&mut connection).await);

        // Now hold exactly 5 response bodies to exhaust the synced semaphore.
        let mut held_bodies = Vec::new();
        for _ in 0..5 {
            let mut c = connection.clone();
            held_bodies.push(send_request(&mut c).await);
        }

        // 6th try_ready must fail (no permits left)
        let mut c6 = connection.clone();
        assert!(
            c6.try_ready().is_none(),
            "expected try_ready to return None at capacity"
        );

        // Drop all held bodies, permits are released
        drop(held_bodies);
    }

    /// With max_concurrent_streams=2, holding two response bodies should
    /// exhaust permits. Dropping one should free a slot.
    #[tokio::test]
    async fn try_ready_fails_at_capacity() {
        let mut connection = Connection::new(TestConnector::new(2), 2);

        // Open two streams, hold both response bodies
        let body1 = send_request(&mut connection).await;
        let body2 = send_request(&mut connection).await;

        // A third try_ready must fail
        let mut c3 = connection.clone();
        assert!(
            c3.try_ready().is_none(),
            "expected try_ready to return None when at capacity"
        );

        // Drop one body, freeing a permit
        drop(body1);

        // Now ready should succeed
        c3.ready().await.unwrap();

        drop(body2);
    }

    /// Multiple tasks sharing a single Connection can send concurrent
    /// requests. The first request triggers the handshake; subsequent
    /// requests wait for it and then reuse the same H2 session.
    #[tokio::test]
    async fn concurrent_requests_on_shared_connection() {
        let connection = Connection::new(TestConnector::new(10), 10);

        let mut handles = JoinSet::default();
        for i in 0u8..5 {
            let mut c = connection.clone();
            handles.spawn(async move {
                c.ready().await.unwrap();
                let resp = c
                    .request(
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

    /// Sends multiple data frames over a streaming request body and reads
    /// back each echoed frame from the response, verifying bidirectional
    /// streaming over an open H2 stream.
    #[tokio::test]
    async fn streaming_request_and_response() {
        let mut connection = Connection::new(TestConnector::new(10), 10);
        connection.ready().await.unwrap();

        let (tx, rx) = mpsc::channel::<Result<Frame<Bytes>, std::convert::Infallible>>(10);
        let resp = connection
            .request(
                Request::builder()
                    .uri("http://test-host:80")
                    .body(http_body_util::StreamBody::new(
                        tokio_stream::wrappers::ReceiverStream::new(rx),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        let mut body = resp.into_body();

        // Send 3 messages, reading the echo after each one
        for i in 0u8..3 {
            let msg = Bytes::from(vec![i; 8]);
            tx.send(Ok(Frame::data(msg.clone()))).await.unwrap();

            let frame = body.frame().await.unwrap().unwrap();
            assert_eq!(
                frame.data_ref().unwrap().as_ref(),
                msg.as_ref(),
                "echo for message {i} should match"
            );
        }

        // Close the request body stream, then expect trailers from the server
        drop(tx);
        let trailer_frame = body.frame().await.unwrap().unwrap();
        assert!(trailer_frame.is_trailers(), "expected trailers frame");

        // Stream should be done
        assert!(body.frame().await.is_none());
    }
}
