// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::table_util::format_using;

use super::schema::LogBuilder;
use restate_types::{
    logs::{
        LogId,
        metadata::{ProviderKind, Segment},
    },
    replicated_loglet::ReplicatedLogletParams,
};
use tracing::debug;

#[inline]
pub(crate) fn append_segment_row(
    builder: &mut LogBuilder,
    output: &mut String,
    id: LogId,
    segment: &Segment<'_>,
) {
    let mut row = builder.row();
    row.id(id.into());
    row.segment_index(segment.config.index().into());
    row.base_lsn(segment.base_lsn.into());
    row.kind(format_using(output, &segment.config.kind));

    if segment.config.kind == ProviderKind::Replicated {
        match ReplicatedLogletParams::deserialize_from(segment.config.params.as_bytes()) {
            Ok(params) => {
                row.loglet_id(params.loglet_id.into());
                row.seq_node_id(params.sequencer.as_plain().into());
                row.seq_node_gen(params.sequencer.generation());
                row.replication(format_using(output, &params.replication));
                row.nodeset(format_using(output, &params.nodeset));
            }
            Err(err) => {
                // this should not happen!
                // todo: should this become an error instead
                debug!("Failed to decode replicated loglet params: {err}")
            }
        }
    }
}
