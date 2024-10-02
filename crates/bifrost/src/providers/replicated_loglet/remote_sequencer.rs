// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{ops::Deref, sync::Arc};

use restate_core::{
    network::{
        rpc_router::{RpcRouter, RpcToken},
        NetworkError, NetworkSendError, Networking, Outgoing, TransportConnect, WeakConnection,
    },
    task_center, ShutdownError, TaskKind,
};
use restate_types::{
    config::Configuration,
    errors::MaybeRetryableError,
    logs::{metadata::SegmentIndex, LogId, Record},
    net::replicated_loglet::{Append, Appended, CommonRequestHeader, SequencerStatus},
    replicated_loglet::ReplicatedLogletParams,
    GenerationalNodeId,
};
use tokio::sync::{mpsc, Mutex, OwnedSemaphorePermit, Semaphore};

use crate::loglet::{AppendError, LogletCommit, LogletCommitResolver, OperationError};

use super::rpc_routers::SequencersRpc;

/// RemoteSequencer
#[derive(Clone)]
pub struct RemoteSequencer<T> {
    log_id: LogId,
    segment_index: SegmentIndex,
    params: ReplicatedLogletParams,
    networking: Networking<T>,
    max_inflight_records_in_config: usize,
    record_permits: Arc<Semaphore>,
    rpc_router: RpcRouter<Append>,
    connection: Arc<Mutex<Option<RemoteSequencerConnection>>>,
}

impl<T> RemoteSequencer<T>
where
    T: TransportConnect,
{
    /// Creates a new instance from SequencerRpc
    ///
    /// Registers all routers into the supplied message router. This ensures that
    /// responses are routed correctly.
    pub fn new(
        log_id: LogId,
        segment_index: SegmentIndex,
        params: ReplicatedLogletParams,
        networking: Networking<T>,
        sequencer_rpc: SequencersRpc,
    ) -> Self {
        let max_inflight_records_in_config: usize = Configuration::pinned()
            .bifrost
            .replicated_loglet
            .maximum_inflight_records
            .into();

        let record_permits = Arc::new(Semaphore::new(max_inflight_records_in_config));

        Self {
            log_id,
            segment_index,
            params,
            networking,
            max_inflight_records_in_config,
            record_permits,
            rpc_router: sequencer_rpc.rpc_router,
            connection: Arc::default(),
        }
    }

    pub async fn append(&self, payloads: Arc<[Record]>) -> Result<LogletCommit, OperationError> {
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

        let permits = self
            .record_permits
            .clone()
            .acquire_many_owned(len)
            .await
            .unwrap();

        let mut connection = self.get_connection().await?;

        let mut msg = Append {
            header: CommonRequestHeader {
                log_id: self.log_id,
                loglet_id: self.params.loglet_id,
                segment_index: self.segment_index,
            },
            payloads: payloads.into(),
        };

        let rpc_token = loop {
            match connection
                .send(&self.rpc_router, self.params.sequencer, msg)
                .await
            {
                Ok(token) => break token,
                Err(err) => {
                    match err.source {
                        NetworkError::ConnectError(_)
                        | NetworkError::ConnectionClosed
                        | NetworkError::Timeout(_) => {
                            // we retry to re-connect one time
                            connection = self.renew_connection(&connection).await?;

                            msg = err.original;
                            continue;
                        }
                        err => return Err(err.into()),
                    }
                }
            };
        };
        let (commit_token, commit_resolver) = LogletCommit::deferred();

        connection.resolve_on_appended(permits, rpc_token, commit_resolver);

        Ok(commit_token)
    }

    /// Gets or starts a new remote sequencer connection
    async fn get_connection(&self) -> Result<RemoteSequencerConnection, NetworkError> {
        let mut guard = self.connection.lock().await;
        if let Some(stream) = guard.deref() {
            return Ok(stream.clone());
        }

        let connection = self
            .networking
            .node_connection(self.params.sequencer.into())
            .await?;
        let stream = RemoteSequencerConnection::start(connection)?;

        *guard = Some(stream.clone());

        Ok(stream)
    }

    /// Renew a connection to a remote sequencer. This grantees that only a single connection
    /// to the sequencer is available.
    async fn renew_connection(
        &self,
        old: &RemoteSequencerConnection,
    ) -> Result<RemoteSequencerConnection, NetworkError> {
        let mut guard = self.connection.lock().await;
        let current = guard.as_ref().expect("connection has been initialized");

        // stream has already been renewed
        if old.inner != current.inner {
            return Ok(current.clone());
        }

        let connection = self
            .networking
            .node_connection(self.params.sequencer.into())
            .await?;

        let stream = RemoteSequencerConnection::start(connection)?;

        *guard = Some(stream.clone());

        Ok(stream)
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
    inner: WeakConnection,
    tx: mpsc::UnboundedSender<RemoteInflightAppend>,
}

impl RemoteSequencerConnection {
    fn start(connection: WeakConnection) -> Result<Self, ShutdownError> {
        let (tx, rx) = mpsc::unbounded_channel();

        task_center().spawn(
            TaskKind::Disposable,
            "remote-sequencer-connection",
            None,
            Self::handle_appended_responses(connection.clone(), rx),
        )?;

        Ok(Self {
            inner: connection,
            tx,
        })
    }

    /// Send append message to remote sequencer.
    ///
    /// It's up to the caller to retry on [`NetworkError`]
    pub async fn send(
        &self,
        rpc_router: &RpcRouter<Append>,
        sequencer: GenerationalNodeId,
        msg: Append,
    ) -> Result<RpcToken<Appended>, NetworkSendError<Append>> {
        let outgoing = Outgoing::new(sequencer, msg).assign_connection(self.inner.clone());

        rpc_router
            .send_on_connection(outgoing)
            .await
            .map_err(|err| NetworkSendError::new(err.original.into_body(), err.source))
    }

    pub fn resolve_on_appended(
        &self,
        permit: OwnedSemaphorePermit,
        rpc_token: RpcToken<Appended>,
        commit_resolver: LogletCommitResolver,
    ) {
        let inflight_append = RemoteInflightAppend {
            rpc_token,
            commit_resolver,
            permit,
        };

        if let Err(err) = self.tx.send(inflight_append) {
            // if we failed to push this to be processed by the connection reactor task
            // then we need to notify the caller
            err.0
                .commit_resolver
                .error(AppendError::retryable(NetworkError::ConnectionClosed));
        }
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
        let mut closed = std::pin::pin!(connection.closed());

        // handle all rpc tokens in a loop, this loop will
        // break only with a break reason.
        // this reason can be one of the following:
        // - Shutdown error
        // - Terminal AppendError
        // - Connection closed
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

                    if let Err(err) = Self::resolve(inflight.commit_resolver, appended.into_body()) {
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
    fn resolve(resolver: LogletCommitResolver, appended: Appended) -> Result<(), AppendError> {
        match appended.status {
            SequencerStatus::Ok => {
                resolver.offset(appended.first_offset);
            }
            SequencerStatus::Sealed => {
                resolver.error(AppendError::Sealed);
                return Err(AppendError::Sealed);
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
    pub status: SequencerStatus,
}

impl MaybeRetryableError for AppendStatusError {
    fn retryable(&self) -> bool {
        !matches!(self.status, SequencerStatus::Malformed)
    }
}

pub enum BreakReason {
    ConnectionClosed,
    AppendError(AppendError),
    Shutdown(ShutdownError),
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

    use crate::{loglet::AppendError, providers::replicated_loglet::rpc_routers::SequencersRpc};

    use super::{RemoteSequencer, RemoteSequencerConnection};
    use rand::Rng;
    use restate_core::{
        network::{rpc_router::RpcRouter, Incoming, MessageHandler, MockConnector},
        TaskCenterBuilder, TestCoreEnv, TestCoreEnvBuilder,
    };
    use restate_types::{
        logs::{LogId, LogletOffset, Record, SequenceNumber},
        net::{
            log_server::Status,
            replicated_loglet::{
                Append, Appended, CommonRequestHeader, CommonResponseHeader, SequencerStatus,
            },
        },
        replicated_loglet::{NodeSet, ReplicatedLogletParams, ReplicationProperty},
        GenerationalNodeId,
    };
    use tokio::sync::Semaphore;

    struct SequencerMockHandler {
        offset: AtomicU32,
        reply_status: SequencerStatus,
    }

    impl SequencerMockHandler {
        fn with_reply_status(reply_status: SequencerStatus) -> Self {
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
                reply_status: SequencerStatus::Ok,
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
        pub remote_sequencer: RemoteSequencer<MockConnector>,
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

        let sequencer_rpc = SequencersRpc::new(&mut builder.router_builder);

        let params = ReplicatedLogletParams {
            loglet_id: 1.into(),
            nodeset: NodeSet::empty(),
            replication: ReplicationProperty::new(1.try_into().unwrap()),
            sequencer: GenerationalNodeId::new(1, 1),
            write_set: None,
        };
        let remote_sequencer = RemoteSequencer::new(
            LogId::new(1),
            1.into(),
            params,
            builder.networking.clone(),
            sequencer_rpc,
        );

        let core_env = builder.build().await;
        core_env
            .tc
            .clone()
            .run_in_scope("test", None, async {
                let env = TestEnv {
                    core_env,
                    remote_sequencer,
                };
                test(env).await;
            })
            .await;
    }

    #[tokio::test]
    async fn test_remote_stream_sealed() {
        let handler = SequencerMockHandler::with_reply_status(SequencerStatus::Sealed);

        setup(handler, |test_env| async move {
            let records: Vec<Record> =
                vec!["record 1".into(), "record 2".into(), "record 3".into()];

            let commit_1 = test_env
                .remote_sequencer
                .append(records.clone().into())
                .await
                .unwrap();

            let commit_2 = test_env
                .remote_sequencer
                .append(records.clone().into())
                .await
                .unwrap();

            let first_offset_1 = commit_1.await;
            assert!(matches!(first_offset_1, Err(AppendError::Sealed)));
            let first_offset_2 = commit_2.await;
            assert!(matches!(first_offset_2, Err(AppendError::Sealed)));
        })
        .await;
    }
}
