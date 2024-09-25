// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{cmp::Ordering, sync::Arc, time::Duration};

use futures::{stream::FuturesUnordered, StreamExt};
use tokio::{sync::OwnedSemaphorePermit, time::timeout};

use restate_core::{
    network::{
        rpc_router::{RpcError, RpcRouter},
        Incoming, NetworkError, Outgoing, TransportConnect,
    },
    Metadata,
};
use restate_types::{
    config::Configuration,
    live::Live,
    logs::{LogletOffset, Record, SequenceNumber, TailState},
    net::log_server::{LogServerResponseHeader, Status, Store, StoreFlags, Stored},
    replicated_loglet::NodeSet,
};

use super::{
    node::{RemoteLogServer, RemoteLogServerManager},
    BatchExt, SequencerSharedState,
};
use crate::{
    loglet::LogletCommitResolver,
    providers::replicated_loglet::replication::spread_selector::SpreadSelector,
};

const DEFAULT_BACKOFF_TIME: Duration = Duration::from_millis(1000);

enum AppenderState {
    Wave {
        // nodes that are known to have already committed the batch
        // currently not used. but should be hinted to the spread
        // selector
        replicated: NodeSet,
        // nodes that should be avoided by the spread selector
        gray_list: NodeSet,
    },
    Done,
    Backoff,
}

/// Appender makes sure a batch of records will run to completion
pub(crate) struct Appender<T> {
    sequencer_shared_state: Arc<SequencerSharedState>,
    log_server_manager: RemoteLogServerManager<T>,
    store_router: RpcRouter<Store>,
    selector: SpreadSelector,
    metadata: Metadata,
    first_offset: LogletOffset,
    records: Arc<[Record]>,
    // permit is held during the entire live
    // of the batch to limit the number of
    // inflight batches
    permit: OwnedSemaphorePermit,
    commit_resolver: LogletCommitResolver,
    configuration: Live<Configuration>,
}

impl<T: TransportConnect> Appender<T> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        sequencer_shared_state: Arc<SequencerSharedState>,
        log_server_manager: RemoteLogServerManager<T>,
        store_router: RpcRouter<Store>,
        selector: SpreadSelector,
        metadata: Metadata,
        first_offset: LogletOffset,
        records: Arc<[Record]>,
        permit: OwnedSemaphorePermit,
        commit_resolver: LogletCommitResolver,
    ) -> Self {
        Self {
            sequencer_shared_state,
            log_server_manager,
            store_router,
            selector,
            metadata,
            first_offset,
            records,
            permit,
            commit_resolver,
            configuration: Configuration::updateable(),
        }
    }

    pub async fn run(mut self) {
        // initial wave has 0 replicated and 0 gray listed node
        let mut state = AppenderState::Wave {
            replicated: NodeSet::empty(),
            gray_list: NodeSet::empty(),
        };

        let retry_policy = self
            .configuration
            .live_load()
            .bifrost
            .replicated_loglet
            .sequencer_backoff_strategy
            .clone();

        let mut retry = retry_policy.iter();

        // note: this should be verified by the sequencer, so this should be safe!
        let next_local_tail = self.records.last_offset(self.first_offset).unwrap().next();

        loop {
            state = match state {
                AppenderState::Done => {
                    self.sequencer_shared_state
                        .global_committed_tail()
                        .notify_offset_update(next_local_tail);
                    self.commit_resolver.offset(next_local_tail);
                    break;
                }
                AppenderState::Wave {
                    replicated,
                    gray_list,
                } => self.wave(replicated, gray_list).await,
                AppenderState::Backoff => {
                    // since backoff can be None, or run out of iterations,
                    // but appender should never give up we fall back to fixed backoff
                    let delay = retry.next().unwrap_or(DEFAULT_BACKOFF_TIME);
                    tokio::time::sleep(delay).await;

                    AppenderState::Wave {
                        // todo: introduce some backoff strategy
                        replicated: NodeSet::empty(),
                        gray_list: NodeSet::empty(),
                    }
                }
            }
        }
    }

    async fn wave(&mut self, replicated: NodeSet, mut gray_list: NodeSet) -> AppenderState {
        // select the spread
        let spread = match self.selector.select(
            &mut rand::thread_rng(),
            &self.metadata.nodes_config_ref(),
            &gray_list,
        ) {
            Ok(spread) => spread,
            Err(_) => {
                if gray_list.is_empty() {
                    // gray list was empty during spread selection!
                    // yet we couldn't find a spread. there is
                    // no reason to retry immediately.
                    return AppenderState::Backoff;
                }
                // otherwise, we retry without a gray list.
                return AppenderState::Wave {
                    replicated,
                    gray_list: NodeSet::empty(),
                };
            }
        };

        let mut gray = false;
        let mut servers = Vec::with_capacity(spread.len());
        for id in spread {
            // at this stage, if we fail to get connection to this server it must be
            // a first time use. We can safely assume this has to be gray listed
            let server = match self.log_server_manager.get(id).await {
                Ok(server) => server,
                Err(err) => {
                    tracing::error!("failed to connect to {}: {}", id, err);
                    gray = true;
                    gray_list.insert(id);
                    continue;
                }
            };

            servers.push(server);
        }

        if gray {
            // Some nodes has been gray listed (wasn't in the original gray list)
            // todo(azmy): is it possible that we still can get a write quorum on the remaining
            //    set of nodes. The selector probably should provide an interface to validate that!

            // we basically try again with a new set of gray_list
            return AppenderState::Wave {
                replicated,
                gray_list,
            };
        }

        // otherwise, we try to send the wave.
        self.send_wave(servers).await
    }

    async fn send_wave(&mut self, spread: Vec<RemoteLogServer>) -> AppenderState {
        let mut senders = FuturesUnordered::new();

        let mut replicated = NodeSet::empty();
        let mut gray_list = NodeSet::empty();

        for server in spread {
            // it is possible that we have visited this server
            // in a previous wave. So we can short circuit here
            // and just skip
            if server.local_tail().latest_offset() > self.first_offset {
                replicated.insert(server.node_id());
                continue;
            }

            // assume gray listed until proved otherwise
            gray_list.insert(server.node_id());

            let task = LogServerStoreTask {
                sequencer_shared_state: Arc::clone(&self.sequencer_shared_state),
                server_manager: self.log_server_manager.clone(),
                server,
                first_offset: self.first_offset,
                records: Arc::clone(&self.records),
                rpc_router: self.store_router.clone(),
            };

            senders.push(task.run());
        }

        loop {
            let result = match timeout(
                self.configuration
                    .live_load()
                    .bifrost
                    .replicated_loglet
                    .log_server_timeout,
                senders.next(),
            )
            .await
            {
                Ok(Some(result)) => result,
                Ok(None) => break, //no more senders
                Err(_err) => {
                    // timedout!
                    // none of the senders has finished in time! we will completely drop this list and then try again
                    return AppenderState::Wave {
                        replicated,
                        gray_list,
                    };
                }
            };

            let (server, response) = result;

            let node_id = server.node_id();
            let response = match response {
                Ok(response) => response,
                Err(err) => {
                    tracing::error!(node_id=%server.node_id(), "failed to send batch to node {}", err);
                    continue;
                }
            };

            // we had a response from this node and there is still a lot we can do
            match response.status {
                Status::Ok => {
                    // only if status is okay that we remove this node
                    // from the gray list, and move to replicated list
                    replicated.insert(node_id);
                    gray_list.remove(&node_id);
                }
                _ => {
                    //todo(azmy): handle other status
                    // note: we don't remove the node from the gray list
                }
            }
        }

        if gray_list.is_empty() {
            // all nodes in the spread has been committed
            AppenderState::Done
        } else {
            AppenderState::Wave {
                replicated,
                gray_list,
            }
        }
    }
}

struct LogServerResponse {
    pub server: RemoteLogServer,
    pub result: Result<Stored, NetworkError>,
}

struct LogServerStoreTask<T> {
    sequencer_shared_state: Arc<SequencerSharedState>,
    server_manager: RemoteLogServerManager<T>,
    server: RemoteLogServer,
    first_offset: LogletOffset,
    records: Arc<[Record]>,
    rpc_router: RpcRouter<Store>,
}

impl<T: TransportConnect> LogServerStoreTask<T> {
    async fn run(mut self) -> (RemoteLogServer, Result<Stored, NetworkError>) {
        let result = self.send().await;
        (self.server, result)
    }

    async fn send(&mut self) -> Result<Stored, NetworkError> {
        let server_local_tail = self
            .server
            .local_tail()
            .wait_for_offset_or_seal(self.first_offset)
            .await?;

        match server_local_tail {
            TailState::Sealed(offset) => {
                let stored = Stored {
                    header: LogServerResponseHeader {
                        local_tail: offset,
                        sealed: true,
                        status: Status::Sealed,
                    },
                };
                //either local or global tail is sealed,
                return Ok(stored);
            }
            TailState::Open(local_tail) => {
                match local_tail.cmp(&self.first_offset) {
                    Ordering::Equal => {}
                    Ordering::Greater => {
                        // somehow the global (or local) offset have moved
                        // behind our first offset!
                        // for now we assume we don't need to make the
                        // write!
                        let stored = Stored {
                            header: LogServerResponseHeader {
                                local_tail,
                                sealed: false,
                                status: Status::Ok,
                            },
                        };
                        return Ok(stored);
                    }
                    Ordering::Less => {}
                };
            }
        }

        let incoming = match self.try_send().await {
            Ok(incoming) => incoming,
            Err(err) => {
                return Err(err);
            }
        };

        // quick actions
        match incoming.body().status {
            Status::Ok => {
                self.server
                    .local_tail()
                    .notify_offset_update(incoming.body().local_tail);
            }
            Status::Sealing | Status::Sealed => {
                self.server.local_tail().notify_seal();
            }
            _ => {}
        }

        Ok(incoming.into_body())
    }

    async fn try_send(&mut self) -> Result<Incoming<Stored>, NetworkError> {
        // if we are here so either we at global committed tail or node tail
        // is at first_offset. In either cases, we can try to send our Store message!
        let store = Store {
            first_offset: self.first_offset,
            flags: StoreFlags::empty(),
            known_archived: LogletOffset::INVALID,
            known_global_tail: self.sequencer_shared_state.committed_tail.latest_offset(),
            loglet_id: self.server.loglet_id(),
            payloads: Vec::from_iter(self.records.iter().cloned()),
            sequencer: self.sequencer_shared_state.node_id,
            timeout_at: None,
        };

        let mut msg = Outgoing::new(self.server.node_id(), store);

        loop {
            let with_connection = msg.assign_connection(self.server.connection().clone());
            match self.rpc_router.call_on_connection(with_connection).await {
                Ok(incoming) => return Ok(incoming),
                Err(RpcError::Shutdown(shutdown)) => return Err(NetworkError::Shutdown(shutdown)),
                Err(RpcError::SendError(err)) => {
                    msg = err.original.forget_connection();

                    match err.source {
                        NetworkError::ConnectionClosed
                        | NetworkError::ConnectError(_)
                        | NetworkError::Timeout(_) => {
                            self.server_manager.renew(&mut self.server).await?
                        }
                        _ => return Err(err.source),
                    }
                }
            }
        }
    }
}
