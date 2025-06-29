// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::Cursor;

use futures::Stream;
use futures_util::stream;

use restate_rocksdb::RocksDbPerfGuard;
use restate_storage_api::deduplication_table::{
    DedupInformation, DedupSequenceNumber, DeduplicationTable, ProducerId,
    ReadOnlyDeduplicationTable,
};
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_storage_api::{Result, StorageError};
use restate_types::identifiers::PartitionId;

use crate::TableKind::Deduplication;
use crate::keys::{KeyKind, TableKey, define_table_key};
use crate::{
    PaddedPartitionId, PartitionStore, PartitionStoreTransaction, StorageAccess, TableScan,
    TableScanIterationDecision,
};

define_table_key!(
    Deduplication,
    KeyKind::Deduplication,
    DeduplicationKey(partition_id: PaddedPartitionId, producer_id: ProducerId)
);

fn get_dedup_sequence_number<S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
    producer_id: &ProducerId,
) -> Result<Option<DedupSequenceNumber>> {
    let _x = RocksDbPerfGuard::new("get-dedup-seq");
    let key = DeduplicationKey::default()
        .partition_id(partition_id.into())
        .producer_id(producer_id.clone());

    storage.get_value(key)
}

fn get_all_sequence_numbers<S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
) -> Result<impl Stream<Item = Result<DedupInformation>> + Send> {
    Ok(stream::iter(storage.for_each_key_value_in_place(
        TableScan::SinglePartition::<DeduplicationKey>(partition_id),
        move |k, mut v| {
            let key =
                DeduplicationKey::deserialize_from(&mut Cursor::new(k)).map(|key| key.producer_id);

            let res = if let Ok(Some(producer_id)) = key {
                DedupSequenceNumber::decode(&mut v).map(|sequence_number| DedupInformation {
                    producer_id,
                    sequence_number,
                })
            } else {
                Err(StorageError::DataIntegrityError)
            };
            TableScanIterationDecision::Emit(res)
        },
    )?))
}

impl ReadOnlyDeduplicationTable for PartitionStore {
    async fn get_dedup_sequence_number(
        &mut self,
        producer_id: &ProducerId,
    ) -> Result<Option<DedupSequenceNumber>> {
        get_dedup_sequence_number(self, self.partition_id(), producer_id)
    }

    fn get_all_sequence_numbers(
        &mut self,
    ) -> Result<impl Stream<Item = Result<DedupInformation>> + Send> {
        get_all_sequence_numbers(self, self.partition_id())
    }
}

impl ReadOnlyDeduplicationTable for PartitionStoreTransaction<'_> {
    async fn get_dedup_sequence_number(
        &mut self,
        producer_id: &ProducerId,
    ) -> Result<Option<DedupSequenceNumber>> {
        get_dedup_sequence_number(self, self.partition_id(), producer_id)
    }

    fn get_all_sequence_numbers(
        &mut self,
    ) -> Result<impl Stream<Item = Result<DedupInformation>> + Send> {
        get_all_sequence_numbers(self, self.partition_id())
    }
}

impl DeduplicationTable for PartitionStoreTransaction<'_> {
    async fn put_dedup_seq_number(
        &mut self,
        producer_id: ProducerId,
        dedup_sequence_number: &DedupSequenceNumber,
    ) -> Result<()> {
        let key = DeduplicationKey::default()
            .partition_id(self.partition_id().into())
            .producer_id(producer_id);

        self.put_kv(key, dedup_sequence_number)
    }
}
