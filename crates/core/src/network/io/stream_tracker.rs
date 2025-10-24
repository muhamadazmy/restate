// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tokio::sync::mpsc::{self, error::TrySendError};

use crate::network::{BidiStreamError, StreamEnvelope};

type Tag = u64;
type DashMap<K, V> = dashmap::DashMap<K, V, ahash::RandomState>;
type StreamSender = mpsc::Sender<StreamEnvelope>;

/// A tracker for responses but can be used to track responses for requests that were dispatched
/// via other mechanisms (e.g. ingress flow)
#[derive(Default)]
pub struct StreamTracker {
    streams: DashMap<Tag, StreamSender>,
}

impl StreamTracker {
    /// Returns None if an in-flight request holds the same msg_id.
    pub fn register_stream(&self, id: Tag, sender: StreamSender) {
        self.streams.insert(id, sender);
    }

    pub fn has_stream(&self, id: Tag) -> bool {
        self.streams.contains_key(&id)
    }

    pub fn pop_stream_sender(&self, id: &Tag) -> Option<StreamSender> {
        self.streams.remove(id).map(|(_, v)| v)
    }

    pub fn handle_envelope(
        &self,
        id: &Tag,
        envelope: StreamEnvelope,
    ) -> Result<(), BidiStreamError> {
        let Some(sender) = self.streams.get(id) else {
            return Err(BidiStreamError::StreamNotFound);
        };

        let result = sender.try_send(envelope).map_err(|err| match err {
            TrySendError::Closed(_) => BidiStreamError::StreamDropped,
            TrySendError::Full(_) => BidiStreamError::LoadShedding,
        });

        // avoid holding a reference into the dashmap
        // before dropping the tracker
        drop(sender);

        if let Err(BidiStreamError::StreamDropped) = &result {
            self.streams.remove(id);
        }

        result
    }
}
