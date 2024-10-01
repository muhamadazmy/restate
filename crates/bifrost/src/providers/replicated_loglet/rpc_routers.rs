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

use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;

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
}

impl SequencersRpc {
    /// Registers all routers into the supplied message router. This ensures that
    /// responses are routed correctly.
    pub fn new(router_builder: &mut MessageRouterBuilder) -> Self {
        let rpc_router = RpcRouter::new(router_builder);

        Self {
            stream: LazyRenewableRemoteSequencerConnection::new(rpc_router),
        }
    }

    pub async fn append<T: TransportConnect>(
        &self,
        loglet: &ReplicatedLoglet<T>,
        payload: Arc<[Record]>,
    ) -> Result<LogletCommit, NetworkError> {
        // loglet.networking()
        let msg = Append {
            header: CommonRequestHeader {
                log_id: loglet.log_id(),
                loglet_id: loglet.params().loglet_id,
                segment_index: loglet.segment_index(),
            },
            payloads: Vec::from_iter(payload.iter().cloned()),
        };

        let mut stream = self.stream.get(loglet.networking()).await?;

        let commit_token = loop {
            // todo(azmy): avoid copying the message on retry
            match stream.send(loglet.params().sequencer, msg.clone()).await {
                Ok(commit_token) => break commit_token,
                Err(err) => {
                    match err {
                        NetworkError::ConnectError(_)
                        | NetworkError::ConnectionClosed
                        | NetworkError::Timeout(_) => {
                            // we retry to re-connect one time
                            stream = self.stream.renew(loglet.networking(), &stream).await?;
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
/// This connection handles are [`Appended`] responses from the remote
/// sequencer. If the connection was lost or if any of the commits failed
/// with a terminal error (like [`Status::Disabled`]) all pending commits
/// are cancelled with an error.
#[derive(Clone)]
struct RemoteSequencerConnection {
    rpc_router: RpcRouter<Append>,
    connection: WeakConnection,
    tx: mpsc::UnboundedSender<RemoteSequencerWaitTask>,
}

impl RemoteSequencerConnection {
    fn start(
        rpc_router: RpcRouter<Append>,
        connection: WeakConnection,
    ) -> Result<Self, ShutdownError> {
        let (tx, rx) = mpsc::unbounded_channel();

        task_center().spawn(
            TaskKind::Disposable,
            "remote-append-stream-",
            None,
            Self::handle_appended_responses(rx),
        )?;

        Ok(Self {
            rpc_router,
            connection,
            tx,
        })
    }

    /// Send append message to remote sequencer.
    ///
    /// It's up to the caller to send to retry on [`NetworkError`]
    pub async fn send(
        &self,
        sequencer: GenerationalNodeId,
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
        let task = RemoteSequencerWaitTask {
            rpc_token,
            commit_resolver,
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
        mut rx: mpsc::UnboundedReceiver<RemoteSequencerWaitTask>,
    ) -> anyhow::Result<()> {
        let mut waiting = FuturesUnordered::new();
        let cancel = CancellationToken::new();

        loop {
            tokio::select! {
                job = rx.recv() => {
                    let job = match job {
                        Some(job) => job,
                        None => break,
                    };

                    waiting.push(Self::wait_and_resolve(cancel.clone(), job));
                },
                Some(result) = waiting.next() => {
                    // this is mainly to drive the waiting for the appended responses.
                    // but on first error we break from this loop because
                    // it means there is an error on connection level and hence all
                    // pending tasks must be cancelled
                    if result.is_err() {
                        // when we break here duo to connection error
                        // all pending tasks will be resolved with retryable error.
                        break;
                    }
                }
            }
        }

        // close connection to stop any further appends
        rx.close();
        cancel.cancel();
        // after cancellation all tasks should resolve immediately
        while waiting.next().await.is_some() {}

        Ok(())
    }

    async fn wait_and_resolve(
        cancel: CancellationToken,
        task: RemoteSequencerWaitTask,
    ) -> Result<(), AppendStreamError> {
        let RemoteSequencerWaitTask {
            rpc_token,
            commit_resolver,
        } = task;

        // wait for response or cancellation
        let appended = tokio::select! {
            incoming = rpc_token.recv() => {
                // returning an error here will cancel the full stream and effectively
                // cancel all resolvers waiting on the same connection
                incoming.map_err(|_| AppendStreamError::ConnectionClosed)?.into_body()
            },
            _ = cancel.cancelled() => {
                // if task is cancelled we assume its duo to closed connection
                // we can resolve the waiting token
                commit_resolver.error(AppendError::retryable(
                    AppendStreamError::ConnectionClosed,
                ));
                return Ok(())
            }
        };

        match appended.status {
            Status::Ok => {
                commit_resolver.offset(appended.first_offset);
            }
            Status::Sealed | Status::Sealing => {
                commit_resolver.sealed();
            }
            Status::Disabled => {
                // this is a special error because it means
                // the server will not accept any more writes
                commit_resolver.error(AppendError::terminal(AppendStreamError::Status(
                    appended.status,
                )));

                return Err(AppendStreamError::Status(appended.status));
            }
            Status::Malformed | Status::OutOfBounds | Status::SequencerMismatch => {
                commit_resolver.error(AppendError::terminal(AppendStreamError::Status(
                    appended.status,
                )));
            }
            Status::Dropped => {
                commit_resolver.error(AppendError::retryable(AppendStreamError::Status(
                    appended.status,
                )));
            }
        }

        Ok(())
    }
}

pub(crate) struct RemoteSequencerWaitTask {
    rpc_token: RpcToken<Appended>,
    commit_resolver: LogletCommitResolver,
}

#[derive(thiserror::Error, Debug)]
pub enum AppendStreamError {
    #[error("connection closed")]
    ConnectionClosed,
    #[error("status {0:?}")]
    Status(Status),
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

    use crate::loglet::AppendError;

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

            let commit_1 = stream
                .send(
                    node_id,
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

            let commit_2 = stream
                .send(
                    node_id,
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

            let commit_1 = stream
                .send(
                    node_id,
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

            let commit_2 = stream
                .send(
                    node_id,
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

            let Err(AppendError::Other(err)) = commit_1.await else {
                panic!("expected error");
            };

            assert!(!err.retryable());

            let Err(AppendError::Other(err)) = commit_2.await else {
                panic!("expected error");
            };

            // this commit is cancelled duo to "connection closed" error
            // hence it's set to be retryable.
            assert!(err.retryable());
        })
        .await;
    }
}
