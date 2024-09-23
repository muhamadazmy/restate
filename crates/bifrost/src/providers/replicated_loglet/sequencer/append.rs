// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{sync::Arc, time::Duration};

use futures::StreamExt;
use tokio::{sync::OwnedSemaphorePermit, task::JoinSet, time::timeout};

use restate_core::{
    network::{rpc_router::ResponseTracker, Incoming, NetworkError, Outgoing},
    Metadata,
};
use restate_types::{
    logs::{LogletOffset, Record, SequenceNumber, TailState},
    net::log_server::{LogServerResponseHeader, Status, Store, StoreFlags, Stored},
    replicated_loglet::NodeSet,
};

use super::{
    node::{RemoteLogServer, RemoteLogServerManager},
    BatchExt, SequencerGlobalState,
};
use crate::{
    loglet::LogletCommitResolver,
    providers::replicated_loglet::replication::spread_selector::SpreadSelector,
};

const SENDER_TIMEOUT: Duration = Duration::from_millis(250);

enum AppenderState {
    Wave {
        replicated: NodeSet,
        gray_list: NodeSet,
    },
    Done,
}

/// Appender makes sure a batch of records will run to completion
pub(crate) struct Appender {
    global: Arc<SequencerGlobalState>,
    log_server_manager: RemoteLogServerManager,
    response_tracker: ResponseTracker<Stored>,
    selector: SpreadSelector,
    metadata: Metadata,
    first_offset: LogletOffset,
    records: Arc<[Record]>,
    // permit is held during the entire live
    // of the batch to limit the number of
    // inflight batches
    permit: OwnedSemaphorePermit,
    resolver: LogletCommitResolver,
}

impl Appender {
    pub fn new(
        global: Arc<SequencerGlobalState>,
        log_server_manager: RemoteLogServerManager,
        response_tracker: ResponseTracker<Stored>,
        selector: SpreadSelector,
        metadata: Metadata,
        first_offset: LogletOffset,
        records: Arc<[Record]>,
        permit: OwnedSemaphorePermit,
        resolver: LogletCommitResolver,
    ) -> Self {
        Self {
            global,
            log_server_manager,
            response_tracker,
            selector,
            metadata,
            first_offset,
            records,
            permit,
            resolver,
        }
    }

    pub async fn run(mut self) {
        let mut state = AppenderState::Wave {
            replicated: NodeSet::empty(),
            gray_list: NodeSet::empty(),
        };

        // note: this should be verified by the sequencer, so this should be safe!
        let tail = self.records.tail(self.first_offset).unwrap();

        loop {
            state = match state {
                AppenderState::Wave {
                    replicated,
                    gray_list,
                } => self.wave(replicated, gray_list).await,
                AppenderState::Done => {
                    self.global.committed_tail().notify_offset_update(tail);
                    self.resolver.offset(tail);
                    break;
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
                //todo(azmy):
                // we will retry without a gray list.
                // but this might get us in an infinite loop!
                // we need a "terminal" break condition to
                // let the sequencer know that this is not possible.
                // it's also possible that the gray list
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
        return self.send_wave(servers).await;
    }

    async fn send_wave(&mut self, spread: Vec<RemoteLogServer>) -> AppenderState {
        let mut senders = JoinSet::new();

        let replication_factor = spread.len();
        let mut replicated = NodeSet::empty();

        let mut gray_list = NodeSet::empty();

        for server in spread {
            // it is possible that we have visited this server
            // in a previous wave. So we can short circuit here
            // and just skip
            if server.tail().latest_offset() > self.first_offset {
                replicated.insert(server.node_id());
                continue;
            }

            // assume gray listed unless proved otherwise
            gray_list.insert(server.node_id());

            let sender = Sender {
                global: Arc::clone(&self.global),
                server_manager: self.log_server_manager.clone(),
                server,
                first_offset: self.first_offset,
                records: Arc::clone(&self.records),
                tracker: self.response_tracker.clone(),
            };

            senders.spawn(sender.run());
        }

        loop {
            let result = match timeout(SENDER_TIMEOUT, senders.join_next()).await {
                Ok(Some(result)) => result,
                Ok(None) => break, //no more senders
                Err(_err) => {
                    // timedout!
                    // none of the senders has finished in time! we will completely drop this list and then try again
                    return AppenderState::Wave {
                        replicated: replicated,
                        gray_list: gray_list,
                    };
                }
            };

            // this will only panic if the sender task was cancelled or panicked
            let (server, response) = result.unwrap();

            let node_id = server.node_id();
            let response = match response {
                Ok(response) => response,
                Err(err) => {
                    tracing::error!(node_id=%server.node_id(), "failed to send batch to node {}", err);
                    gray_list.insert(node_id);
                    continue;
                }
            };

            // we had a response from this node and there is still a lot we can do
            match response.status {
                Status::Ok => {
                    replicated.insert(node_id);
                    gray_list.remove(&node_id);
                }
                _ => {
                    //todo(azmy): handle other status
                    // note: we don't remove the node from the gray list
                }
            }
        }

        if gray_list.is_empty() && replicated.len() == replication_factor {
            AppenderState::Done
        } else {
            AppenderState::Wave {
                replicated,
                gray_list,
            }
        }
    }
}

struct Sender {
    global: Arc<SequencerGlobalState>,
    server_manager: RemoteLogServerManager,
    server: RemoteLogServer,
    first_offset: LogletOffset,
    records: Arc<[Record]>,
    tracker: ResponseTracker<Stored>,
}

impl Sender {
    async fn run(mut self) -> (RemoteLogServer, Result<Stored, NetworkError>) {
        let result = self.send().await;
        (self.server, result)
    }

    async fn send(&mut self) -> Result<Stored, NetworkError> {
        let mut global_tail = self.global.committed_tail().to_stream();
        let mut local_tail = self.server.tail().to_stream();

        loop {
            let tail = tokio::select! {
                Some(tail) = global_tail.next() => {
                    tail
                },
                Some(tail) = local_tail.next() => {
                    tail
                }
            };

            match tail {
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
                TailState::Open(offset) => {
                    if offset > self.first_offset {
                        // somehow the global (or local) offset have moved
                        // behind our first offset!
                        // for now we assume we don't need to make the
                        // write!
                        let stored = Stored {
                            header: LogServerResponseHeader {
                                local_tail: offset,
                                sealed: false,
                                status: Status::Ok,
                            },
                        };
                        return Ok(stored);
                    } else if offset == self.first_offset {
                        // it's our turn to execute the write now
                        break;
                    }
                }
            }
        }

        let incoming = match self.try_send().await {
            Ok(incoming) => incoming,
            Err(err) => {
                return Err(err);
            }
        };

        // quick actions
        match incoming.status {
            Status::Ok => {
                self.server.tail().notify_offset_update(incoming.local_tail);
            }
            Status::Sealing | Status::Sealed => {
                self.server.tail().notify_seal();
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
            known_global_tail: self.global.committed_tail.latest_offset(),
            loglet_id: self.server.loglet_id(),
            payloads: Vec::from_iter(self.records.iter().cloned()),
            sequencer: self.global.node_id,
            timeout_at: None,
        };

        let mut msg = Outgoing::new(self.server.node_id(), store);
        let token = self
            .tracker
            .new_token(msg.msg_id())
            .expect("unique message id");

        loop {
            match self.server.sender().send(msg).await {
                Ok(_) => break,
                Err(send) => {
                    msg = send.message;

                    match send.source {
                        NetworkError::ConnectionClosed
                        | NetworkError::ConnectError(_)
                        | NetworkError::Timeout(_) => {
                            self.server_manager.renew(&mut self.server).await?
                        }
                        _ => return Err(send.source.into()),
                    }
                }
            }
        }

        // message has been sent! there is noway
        // we are sure it has been received by the other peer
        // so we wait for response. indefinitely
        // it's up to the appender time outs to try again
        token.recv().await.map_err(NetworkError::from)
    }
}
