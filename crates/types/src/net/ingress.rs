// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use crate::invocation::ServiceInvocation;
use crate::net::partition_processor::PartitionLeaderService;
use crate::net::{bilrost_wire_codec, default_wire_codec, define_rpc};

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub enum IngestDedup {
    #[default]
    None,
    Producer {
        producer_id: u128,
        offset: u64,
    },
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct IngestRecord {
    pub dedup: IngestDedup,
    // todo: this is probably need to be serialized
    // bytes of the Envelope
    // which means the dedup information
    // is already built in.
    pub invocation: Box<ServiceInvocation>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct IngestRequest {
    pub records: Arc<[IngestRecord]>,
}

impl From<Arc<[IngestRecord]>> for IngestRequest {
    fn from(records: Arc<[IngestRecord]>) -> Self {
        Self { records }
    }
}

// todo(azmy): Use bilrost (depends on the payload)
default_wire_codec!(IngestRequest);

#[derive(Debug, bilrost::Oneof, bilrost::Message)]
pub enum IngestResponse {
    Unknown,
    #[bilrost(tag = 1, message)]
    Ack,
}

bilrost_wire_codec!(IngestResponse);

define_rpc! {
    @request=IngestRequest,
    @response=IngestResponse,
    @service=PartitionLeaderService,
}
