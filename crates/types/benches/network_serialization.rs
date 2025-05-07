// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{sync::Arc, time::Duration};

use bilrost::{Message, OwnedMessage};
use criterion::{Criterion, black_box, criterion_group, criterion_main};

use restate_types::{
    cluster::cluster_state::{PartitionProcessorStatus, ReplayStatus, RunMode},
    identifiers::PartitionId,
    logs::Lsn,
    net::{
        metadata::{MetadataContainer, MetadataUpdate},
        node::NodeStateResponse,
    },
    schema::Schema,
    time::MillisSinceEpoch,
};

const SCHEMA_METADATA: &[u8] = include_bytes!("./assets/schema_dump.json");

const NUM_PARTITIONS: u16 = 256;

pub fn gen_partition_processor_status() -> PartitionProcessorStatus {
    let run_mode = || {
        if rand::random_range(0..=1) == 0 {
            RunMode::Follower
        } else {
            RunMode::Leader
        }
    };

    let lsn = || Some(Lsn::new(rand::random()));

    PartitionProcessorStatus {
        updated_at: MillisSinceEpoch::new(rand::random()),
        effective_mode: run_mode(),
        planned_mode: run_mode(),
        replay_status: ReplayStatus::Active,
        target_tail_lsn: lsn(),
        last_applied_log_lsn: lsn(),
        last_archived_log_lsn: lsn(),
        last_persisted_log_lsn: lsn(),
        ..Default::default()
    }
}

pub fn gen_node_state_response() -> NodeStateResponse {
    NodeStateResponse {
        partition_processor_state: Some(
            (0..NUM_PARTITIONS)
                .map(|id| (PartitionId::from(id), gen_partition_processor_status()))
                .collect(),
        ),
        uptime: Duration::from_secs(rand::random_range(500..1000)),
    }
}

pub fn flexbuffer_node_state_serialization(c: &mut Criterion) {
    let message = gen_node_state_response();

    let serialized = flexbuffers::to_vec(&message).expect("serializes");
    println!("Flexbuffers Message Size: {}", serialized.len());

    c.bench_function("flexbuffers-node-state-serialize", |b| {
        b.iter(|| black_box(flexbuffers::to_vec(&message)));
    });

    c.bench_function("flexbuffers-node-state-deserialize", |b| {
        b.iter(|| {
            black_box(
                flexbuffers::from_slice::<NodeStateResponse>(&serialized).expect("deserializes"),
            )
        });
    });
}

pub fn bilrost_node_state_serialization(c: &mut Criterion) {
    let message = gen_node_state_response();

    let serialized = message.encode_to_bytes();
    println!("Bilrost Message Size: {}", serialized.len());
    c.bench_function("bilrost-node-state-serialize", |b| {
        b.iter(|| black_box(message.encode_to_bytes()));
    });

    c.bench_function("bilrost-node-state-deserialize", |b| {
        b.iter(|| {
            black_box(<NodeStateResponse as OwnedMessage>::decode(
                serialized.clone(),
            ))
        });
    });
}

pub fn flexbuffer_schema_metadata_serialization(c: &mut Criterion) {
    let schema: Schema = serde_json::from_slice(SCHEMA_METADATA).expect("valid schema object");

    let message = MetadataUpdate {
        container: MetadataContainer::Schema(Arc::new(schema)),
    };

    let serialized = flexbuffers::to_vec(&message).expect("serializes");
    println!("Flexbuffers Message Size: {}", serialized.len());

    c.bench_function("flexbuffers-schema-metadata-serialize", |b| {
        b.iter(|| black_box(flexbuffers::to_vec(&message)));
    });

    c.bench_function("flexbuffers-schema-metadata-deserialize", |b| {
        b.iter(|| {
            black_box(
                flexbuffers::from_slice::<MetadataUpdate>(&(serialized.clone()))
                    .expect("deserializes"),
            )
        });
    });
}

pub fn bilrost_schema_metadata_serialization(c: &mut Criterion) {
    let schema: Schema = serde_json::from_slice(SCHEMA_METADATA).expect("valid schema object");

    let message = MetadataUpdate {
        container: MetadataContainer::Schema(Arc::new(schema)),
    };

    let rev = message.encode_fast();
    let serialized = bytes::Bytes::from(rev.into_vec());
    // let serialized = message.encode_to_bytes();
    println!("Bilrost Message Size: {}", serialized.len());
    c.bench_function("bilrost-schema-metadata-serialize", |b| {
        b.iter(|| {
            let buf = black_box(message.encode_fast());
            black_box(buf.into_vec());
        });
    });

    c.bench_function("bilrost-schema-metadata-deserialize", |b| {
        b.iter(|| black_box(<MetadataUpdate as OwnedMessage>::decode(serialized.clone())));
    });
}

criterion_group!(
    name=benches;
    config=Criterion::default();
    targets=flexbuffer_schema_metadata_serialization, bilrost_schema_metadata_serialization
);

criterion_main!(benches);
