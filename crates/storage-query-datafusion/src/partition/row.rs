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

use super::schema::PartitionBuilder;
use itertools::{Itertools, Position};
use restate_types::partition_table::Partition;

#[inline]
pub(crate) fn append_partition_rows(
    builder: &mut PartitionBuilder,
    output: &mut String,
    partition: &Partition,
) {
    for (position, node_id) in partition.placement.iter().cloned().with_position() {
        let mut row = builder.row();
        row.partition_id(partition.partition_id.into());
        row.plain_node_id(format_using(output, &node_id));
        row.start_key(*partition.key_range.start());
        row.end_key(*partition.key_range.end());

        match position {
            Position::First | Position::Only => {
                row.target_mode("LEADER");
            }
            _ => {
                row.target_mode("FOLLOWER");
            }
        }
    }
}
