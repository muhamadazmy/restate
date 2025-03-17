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
use restate_types::logs::{LogId, metadata::Segment};

#[inline]
pub(crate) fn append_segment_row<'a>(
    builder: &mut LogBuilder,
    output: &mut String,
    id: LogId,
    segment: &Segment<'a>,
) {
    let mut row = builder.row();
    row.id(id.into());
    row.segment_index(segment.config.index().into());
    row.base_lsn(segment.base_lsn.into());
    row.kind(format_using(output, &segment.config.kind));
}
