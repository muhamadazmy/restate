// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// todo(asoli): remove once this is used
#![allow(dead_code)]

use std::ops::Deref;
use std::sync::Arc;

use restate_types::config::Configuration;
use restate_types::errors::MaybeRetryableError;
use tokio::sync::{mpsc, Mutex, OwnedSemaphorePermit, Semaphore};

use restate_core::network::rpc_router::{RpcRouter, RpcToken};
use restate_core::network::{
    MessageRouterBuilder, NetworkError, Networking, Outgoing, TransportConnect, WeakConnection,
};
use restate_core::{task_center, ShutdownError, TaskKind};
use restate_types::logs::Record;
use restate_types::net::log_server::{
    GetLogletInfo, GetRecords, Release, Seal, Status, Store, Trim,
};
use restate_types::net::replicated_loglet::{Append, Appended, CommonRequestHeader};
use restate_types::GenerationalNodeId;

use super::loglet::ReplicatedLoglet;
use crate::loglet::{AppendError, LogletCommit, LogletCommitResolver};

/// Used by replicated loglets to send requests and receive responses from log-servers
/// Cloning this is cheap and all clones will share the same internal trackers.
#[derive(Clone)]
pub struct LogServersRpc {
    pub store: RpcRouter<Store>,
    pub release: RpcRouter<Release>,
    pub trim: RpcRouter<Trim>,
    pub seal: RpcRouter<Seal>,
    pub get_loglet_info: RpcRouter<GetLogletInfo>,
    pub get_records: RpcRouter<GetRecords>,
}

impl LogServersRpc {
    /// Registers all routers into the supplied message router. This ensures that
    /// responses are routed correctly.
    pub fn new(router_builder: &mut MessageRouterBuilder) -> Self {
        let store = RpcRouter::new(router_builder);
        let release = RpcRouter::new(router_builder);
        let trim = RpcRouter::new(router_builder);
        let seal = RpcRouter::new(router_builder);
        let get_loglet_info = RpcRouter::new(router_builder);
        let get_records = RpcRouter::new(router_builder);

        Self {
            store,
            release,
            trim,
            seal,
            get_loglet_info,
            get_records,
        }
    }
}

/// Used by replicated loglets to send requests and receive responses from sequencers (other nodes
/// running replicated loglets)
/// Cloning this is cheap and all clones will share the same internal trackers.
#[derive(Clone)]
pub struct SequencersRpc {
    stream: LazyRenewableRemoteSequencerConnection,
    max_inflight_records_in_config: usize,
    record_permits: Arc<Semaphore>,
}

impl SequencersRpc {
    /// Creates a new instance from SequencerRpc
    ///
    /// Registers all routers into the supplied message router. This ensures that
    /// responses are routed correctly.
    pub fn new(router_builder: &mut MessageRouterBuilder) -> Self {
        let rpc_router = RpcRouter::new(router_builder);

        let max_inflight_records_in_config: usize = Configuration::pinned()
            .bifrost
            .replicated_loglet
            .maximum_inflight_records
            .into();

        let record_permits = Arc::new(Semaphore::new(max_inflight_records_in_config));

        Self {
            stream: LazyRenewableRemoteSequencerConnection::new(rpc_router),
            max_inflight_records_in_config,
            record_permits,
        }
    }

    pub async fn append<T: TransportConnect>(
        &self,
        loglet: &ReplicatedLoglet<T>,
        payloads: Arc<[Record]>,
    ) -> Result<LogletCommit, NetworkError> {
        if payloads.len() > self.max_inflight_records_in_config {
            let delta = payloads.len() - self.max_inflight_records_in_config;
            tracing::debug!(
                "Resizing sequencer in-flight records capacity to allow admission for this batch. \
                Capacity in configuration is {} and we are adding capacity of {} to it",
                self.max_inflight_records_in_config,
                delta
            );
            self.record_permits.add_permits(delta);
        }

        let len = u32::try_from(payloads.len()).expect("batch sizes fit in u32");

        let mut connection = self.stream.get(loglet.networking()).await?;

        let msg = Append {
            header: CommonRequestHeader {
                log_id: loglet.log_id(),
                loglet_id: loglet.params().loglet_id,
                segment_index: loglet.segment_index(),
            },
            payloads: Vec::from_iter(payloads.iter().cloned()),
        };

        let commit_token = loop {
            let permits = self
                .record_permits
                .clone()
                .acquire_many_owned(len)
                .await
                .unwrap();

            // todo(azmy): avoid copying all records on retry
            match connection
                .send(loglet.params().sequencer, permits, msg.clone())
                .await
            {
                Ok(commit_token) => break commit_token,
                Err(err) => {
                    match err {
                        NetworkError::ConnectError(_)
                        | NetworkError::ConnectionClosed
                        | NetworkError::Timeout(_) => {
                            // we retry to re-connect one time
                            connection =
                                self.stream.renew(loglet.networking(), &connection).await?;
                        }
                        err => return Err(err),
                    }
                }
            }
        };

        Ok(commit_token)
    }
}

/// RemoteSequencerStream represents a single open connection
/// to remote leader sequencer.
///
/// This connection handles all [`Appended`] responses from the remote
/// sequencer.
///
/// If the connection was lost or if any of the commits failed
/// with a terminal error (like [`Status::Disabled`]) all pending commits
/// are resolved with an error.
#[derive(Clone)]
struct RemoteSequencerConnection {
    rpc_router: RpcRouter<Append>,
    connection: WeakConnection,
    tx: mpsc::UnboundedSender<RemoteInflightAppend>,
}

impl RemoteSequencerConnection {
    fn start(
        rpc_router: RpcRouter<Append>,
        connection: WeakConnection,
    ) -> Result<Self, ShutdownError> {
        let (tx, rx) = mpsc::unbounded_channel();

        task_center().spawn(
            TaskKind::Disposable,
            "remote-sequencer-connection",
            None,
            Self::handle_appended_responses(connection.clone(), rx),
        )?;

        Ok(Self {
            rpc_router,
            connection,
            tx,
        })
    }

    /// Send append message to remote sequencer.
    ///
    /// It's up to the caller to retry on [`NetworkError`]
    pub async fn send(
        &self,
        sequencer: GenerationalNodeId,
        permit: OwnedSemaphorePermit,
        msg: Append,
    ) -> Result<LogletCommit, NetworkError> {
        if self.tx.is_closed() {
            return Err(NetworkError::ConnectionClosed);
        }

        let outgoing = Outgoing::new(sequencer, msg).assign_connection(self.connection.clone());

        let rpc_token = self
            .rpc_router
            .send_on_connection(outgoing)
            .await
            .map_err(|err| err.source)?;

        let (commit, commit_resolver) = LogletCommit::deferred();
        let task = RemoteInflightAppend {
            rpc_token,
            commit_resolver,
            permit,
        };

        // wait for response asynchronously
        self.tx
            .send(task)
            .map_err(|_| NetworkError::ConnectionClosed)?;

        Ok(commit)
    }

    /// Handle all [`Appended`] responses
    ///
    /// This task will run until the [`AppendStream`] is dropped. Once dropped
    /// all pending commits will be resolved with an error. it's up to the enqueuer
    /// to retry if needed.
    async fn handle_appended_responses(
        connection: WeakConnection,
        mut rx: mpsc::UnboundedReceiver<RemoteInflightAppend>,
    ) -> anyhow::Result<()> {
        // let mut waiting = VecDeque::new();

        let closed = connection.closed();
        tokio::pin!(closed);

        // handle all rpc tokens in a loop, this loop will
        // break only with a break reason.
        // this reason can be one of the following:
        // - Shutdown error
        // - Terminal AppendError, for example,
        let reason = loop {
            let inflight = match rx.recv().await {
                Some(inflight) => inflight,
                None => {
                    break BreakReason::Shutdown(ShutdownError);
                }
            };

            tokio::select! {
                appended = inflight.rpc_token.recv() => {
                    let appended = match appended {
                        Ok(appended) => appended,
                        Err(err) => {
                            break BreakReason::Shutdown(err);
                        }
                    };

                    if let Err(err) = Self::resolve(inflight.commit_resolver, appended.body()) {
                        break BreakReason::AppendError(err);
                    }
                },
                _ = &mut closed => {
                    break BreakReason::ConnectionClosed;
                }
            }
        };

        // close channel to stop any further appends calls on the same connection

        rx.close();
        // drain and resolve ALL pending appends on this connection.
        while let Some(inflight) = rx.recv().await {
            match reason {
                BreakReason::Shutdown(_) => {
                    inflight
                        .commit_resolver
                        .error(AppendError::terminal(ShutdownError));
                }
                BreakReason::ConnectionClosed => inflight
                    .commit_resolver
                    .error(AppendError::retryable(NetworkError::ConnectionClosed)),
                BreakReason::AppendError(ref err) => {
                    inflight.commit_resolver.error(err.clone());
                }
            }
        }

        Ok(())
    }

    /// Wait for [`Appended`] responses from remote sequencer. Then
    /// resolve the [`LogletCommit`] with proper response.
    ///
    /// On cancellation, the commit is resolved with a retryable error.
    fn resolve(resolver: LogletCommitResolver, appended: &Appended) -> Result<(), AppendError> {
        match appended.status {
            Status::Ok => {
                resolver.offset(appended.first_offset);
            }
            Status::Sealed | Status::Sealing => {
                resolver.error(AppendError::Sealed);
                return Err(AppendError::Sealed);
            }
            Status::Disabled => {
                // this is a terminal status. Connection is not usable afterwards
                let err = AppendError::other(AppendStatusError {
                    status: appended.status,
                });
                resolver.error(err.clone());
                // this is a terminal error for connection.
                return Err(err);
            }
            _ => {
                // we can resolve this commit with an error, but this doesn't
                // invalidate the entire connection.
                resolver.error(AppendError::other(AppendStatusError {
                    status: appended.status,
                }));
            }
        };

        Ok(())
    }
}

//todo: inflight remote append
pub(crate) struct RemoteInflightAppend {
    rpc_token: RpcToken<Appended>,
    commit_resolver: LogletCommitResolver,
    permit: OwnedSemaphorePermit,
}

#[derive(thiserror::Error, Debug, Clone)]
#[error("error status received: {status:?}")]
pub struct AppendStatusError {
    pub status: Status,
}

impl MaybeRetryableError for AppendStatusError {
    fn retryable(&self) -> bool {
        !matches!(
            self.status,
            Status::Disabled
                | Status::Malformed
                | Status::OutOfBounds
                | Status::SequencerMismatch
                | Status::Sealed
                | Status::Sealing
        )
    }
}

pub enum BreakReason {
    ConnectionClosed,
    AppendError(AppendError),
    Shutdown(ShutdownError),
}

/// Maintains a single connection to remote sequencer.
///
/// If the underlying connection failed, all pending commits
/// on the same stream are immediately cancelled with an error.
///
/// It's up to the enqueuer to retry if the error is retryable.
#[derive(Clone)]
struct LazyRenewableRemoteSequencerConnection {
    append: RpcRouter<Append>,
    connection: Arc<Mutex<Option<RemoteSequencerConnection>>>,
}

impl LazyRenewableRemoteSequencerConnection {
    pub fn new(rpc_router: RpcRouter<Append>) -> Self {
        Self {
            append: rpc_router,
            connection: Arc::default(),
        }
    }

    /// Gets or starts a new remote sequencer connection
    async fn get<T: TransportConnect>(
        &self,
        networking: &Networking<T>,
    ) -> Result<RemoteSequencerConnection, NetworkError> {
        let mut guard = self.connection.lock().await;
        if let Some(stream) = guard.deref() {
            return Ok(stream.clone());
        }

        let connection = networking
            .node_connection(networking.metadata().my_node_id().into())
            .await?;
        let stream = RemoteSequencerConnection::start(self.append.clone(), connection)?;

        *guard = Some(stream.clone());

        Ok(stream)
    }

    /// Renew a connection to a remote sequencer. This grantees that only a single connection
    /// to the sequencer is available.
    async fn renew<T: TransportConnect>(
        &self,
        networking: &Networking<T>,
        old: &RemoteSequencerConnection,
    ) -> Result<RemoteSequencerConnection, NetworkError> {
        let mut guard = self.connection.lock().await;
        let current = guard.as_ref().expect("connection has been initialized");

        // stream has already been renewed
        if old.connection != current.connection {
            return Ok(current.clone());
        }

        let connection = networking
            .node_connection(networking.metadata().my_node_id().into())
            .await?;

        let stream = RemoteSequencerConnection::start(self.append.clone(), connection)?;

        *guard = Some(stream.clone());

        Ok(stream)
    }
}

#[cfg(test)]
mod test {
    use std::{
        future::Future,
        sync::{
            atomic::{AtomicU32, Ordering},
            Arc,
        },
        time::Duration,
    };

    use super::RemoteSequencerConnection;
    use rand::Rng;
    use restate_core::{
        network::{rpc_router::RpcRouter, Incoming, MessageHandler, MockConnector},
        TaskCenterBuilder, TestCoreEnv, TestCoreEnvBuilder,
    };
    use restate_types::{
        logs::{LogId, LogletOffset, Record, SequenceNumber},
        net::{
            log_server::Status,
            replicated_loglet::{Append, Appended, CommonRequestHeader, CommonResponseHeader},
        },
    };
    use tokio::sync::Semaphore;

    struct SequencerMockHandler {
        offset: AtomicU32,
        reply_status: Status,
    }

    impl SequencerMockHandler {
        fn with_reply_status(reply_status: Status) -> Self {
            Self {
                reply_status,
                ..Default::default()
            }
        }
    }

    impl Default for SequencerMockHandler {
        fn default() -> Self {
            Self {
                offset: AtomicU32::new(LogletOffset::OLDEST.into()),
                reply_status: Status::Ok,
            }
        }
    }

    impl MessageHandler for SequencerMockHandler {
        type MessageType = Append;
        async fn on_message(&self, msg: Incoming<Self::MessageType>) {
            let first_offset = self
                .offset
                .fetch_add(msg.body().payloads.len() as u32, Ordering::Relaxed);

            let outgoing = msg.into_outgoing(Appended {
                first_offset: LogletOffset::from(first_offset),
                header: CommonResponseHeader {
                    known_global_tail: None,
                    sealed: Some(false),
                    status: self.reply_status,
                },
            });
            let delay = rand::thread_rng().gen_range(50..350);
            tokio::time::sleep(Duration::from_millis(delay)).await;
            outgoing.send().await.unwrap();
        }
    }

    struct TestEnv {
        pub core_env: TestCoreEnv<MockConnector>,
        pub rpc: RpcRouter<Append>,
    }
    async fn setup<F, O>(sequencer: SequencerMockHandler, test: F)
    where
        O: Future<Output = ()>,
        F: FnOnce(TestEnv) -> O,
    {
        let (connector, _receiver) = MockConnector::new(100);
        let tc = TaskCenterBuilder::default()
            .default_runtime_handle(tokio::runtime::Handle::current())
            .ingress_runtime_handle(tokio::runtime::Handle::current())
            .build()
            .expect("task_center builds");
        let connector = Arc::new(connector);

        let mut builder =
            TestCoreEnvBuilder::with_transport_connector(tc.clone(), Arc::clone(&connector))
                .add_mock_nodes_config()
                .add_message_handler(sequencer);

        let rpc = RpcRouter::new(&mut builder.router_builder);

        let core_env = builder.build().await;
        core_env
            .tc
            .clone()
            .run_in_scope("test", None, async {
                let env = TestEnv { core_env, rpc };
                test(env).await;
            })
            .await;
    }

    #[tokio::test]
    async fn test_remote_stream_success() {
        let handler = SequencerMockHandler::default();
        let permits = Arc::new(Semaphore::new(10));

        setup(handler, |test_env| async move {
            let node_id = test_env.core_env.networking.my_node_id();

            let connection = test_env
                .core_env
                .networking
                .node_connection(node_id.into())
                .await
                .unwrap();

            let stream =
                RemoteSequencerConnection::start(test_env.rpc.clone(), connection).unwrap();

            let records: Vec<Record> =
                vec!["record 1".into(), "record 2".into(), "record 3".into()];

            let permit = permits.clone().acquire_owned().await.unwrap();
            let commit_1 = stream
                .send(
                    node_id,
                    permit,
                    Append {
                        header: CommonRequestHeader {
                            log_id: LogId::new(1),
                            loglet_id: 1.into(),
                            segment_index: 1.into(),
                        },
                        payloads: records.clone(),
                    },
                )
                .await
                .unwrap();

            let permit = permits.clone().acquire_owned().await.unwrap();
            let commit_2 = stream
                .send(
                    node_id,
                    permit,
                    Append {
                        header: CommonRequestHeader {
                            log_id: LogId::new(1),
                            loglet_id: 1.into(),
                            segment_index: 1.into(),
                        },
                        payloads: records,
                    },
                )
                .await
                .unwrap();

            let first_offset_1 = commit_1.await.unwrap();
            let first_offset_2 = commit_2.await.unwrap();

            assert_eq!(first_offset_1, LogletOffset::OLDEST);
            assert_eq!(first_offset_2, LogletOffset::new(4));
        })
        .await;
    }

    #[tokio::test]
    async fn test_remote_stream_dropped() {
        // we simulate a terminal sequencer failure by returning a Disabled status
        let handler = SequencerMockHandler::with_reply_status(Status::Disabled);
        let permits = Arc::new(Semaphore::new(10));

        setup(handler, |test_env| async move {
            let node_id = test_env.core_env.networking.my_node_id();

            let connection = test_env
                .core_env
                .networking
                .node_connection(node_id.into())
                .await
                .unwrap();

            let stream =
                RemoteSequencerConnection::start(test_env.rpc.clone(), connection).unwrap();

            let records: Vec<Record> =
                vec!["record 1".into(), "record 2".into(), "record 3".into()];

            let permit = permits.clone().acquire_owned().await.unwrap();
            let commit_1 = stream
                .send(
                    node_id,
                    permit,
                    Append {
                        header: CommonRequestHeader {
                            log_id: LogId::new(1),
                            loglet_id: 1.into(),
                            segment_index: 1.into(),
                        },
                        payloads: records.clone(),
                    },
                )
                .await
                .unwrap();

            let permit = permits.clone().acquire_owned().await.unwrap();

            let commit_2 = stream
                .send(
                    node_id,
                    permit,
                    Append {
                        header: CommonRequestHeader {
                            log_id: LogId::new(1),
                            loglet_id: 1.into(),
                            segment_index: 1.into(),
                        },
                        payloads: records,
                    },
                )
                .await
                .unwrap();

            //todo(azmy): check type of error
            assert!(commit_1.await.is_err());
            assert!(commit_2.await.is_err());

            assert_eq!(permits.available_permits(), 10);
        })
        .await;
    }
}
