// Copyright (c) 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use metrics::{describe_counter, describe_histogram, Unit};

pub(crate) const METADATA_SERVER_GET_DURATION: &str = "restate.metadata_server.get.duration";
pub(crate) const METADATA_SERVER_GET_VERSION_DURATION: &str =
    "restate.metadata_server.get_version.duration";
pub(crate) const METADATA_SERVER_PUT_DURATION: &str = "restate.metadata_server.put/duration";
pub(crate) const METADATA_SERVER_DELETE_DURATION: &str = "restate.metadata_server.delete.duration";

pub(crate) const METADATA_SERVER_GET_TOTAL: &str = "restate.metadata_server.get.total";
pub(crate) const METADATA_SERVER_GET_VERSION_TOTAL: &str =
    "restate.metadata_server.get_version.total";
pub(crate) const METADATA_SERVER_PUT_TOTAL: &str = "restate.metadata_server.put.total";
pub(crate) const METADATA_SERVER_DELETE_TOTAL: &str = "restate.metadata_server.delete.total";

pub(crate) const METADATA_SERVER_RAFT_SENT_MESSAGE_TOTAL: &str =
    "restate.metadata_server.raft.sent_messages.total";
pub(crate) const METADATA_SERVER_RAFT_RECV_MESSAGE_TOTAL: &str =
    "restate.metadata_server.raft.received_messages.total";
pub(crate) const METADATA_SERVER_RAFT_SENT_MESSAGE_BYTES: &str =
    "restate.metadata_server.raft.sent_messages.bytes";
pub(crate) const METADATA_SERVER_RAFT_RECV_MESSAGE_BYTES: &str =
    "restate.metadata_server.raft.received_messages.bytes";

pub(crate) fn describe_metrics() {
    describe_histogram!(
        METADATA_SERVER_GET_DURATION,
        Unit::Seconds,
        "Metadata get request duration in seconds as measured by the metadata handler"
    );

    describe_histogram!(
        METADATA_SERVER_GET_VERSION_DURATION,
        Unit::Seconds,
        "Metadata get_version request duration in seconds as measured by the metadata handler"
    );

    describe_histogram!(
        METADATA_SERVER_PUT_DURATION,
        Unit::Seconds,
        "Metadata put request duration in seconds as measured by the metadata handler"
    );

    describe_histogram!(
        METADATA_SERVER_DELETE_DURATION,
        Unit::Seconds,
        "Metadata delete request duration in seconds as measured by the metadata handler"
    );

    describe_counter!(
        METADATA_SERVER_GET_TOTAL,
        Unit::Count,
        "Metadata get request count as measured by the metadata handler"
    );

    describe_counter!(
        METADATA_SERVER_GET_VERSION_TOTAL,
        Unit::Count,
        "Metadata get_version request count as measured by the metadata handler"
    );

    describe_counter!(
        METADATA_SERVER_PUT_TOTAL,
        Unit::Count,
        "Metadata put request count as measured by the metadata handler"
    );

    describe_counter!(
        METADATA_SERVER_DELETE_TOTAL,
        Unit::Count,
        "Metadata delete request count as measured by the metadata handler"
    );

    describe_counter!(
        METADATA_SERVER_RAFT_SENT_MESSAGE_TOTAL,
        Unit::Count,
        "Raft Metadata server sent messages count"
    );

    describe_counter!(
        METADATA_SERVER_RAFT_RECV_MESSAGE_TOTAL,
        Unit::Count,
        "Raft Metadata server received messages count"
    );

    describe_counter!(
        METADATA_SERVER_RAFT_SENT_MESSAGE_BYTES,
        Unit::Bytes,
        "Raft Metadata server sent messages size in bytes"
    );

    describe_counter!(
        METADATA_SERVER_RAFT_RECV_MESSAGE_BYTES,
        Unit::Bytes,
        "Raft Metadata server received messages size in bytes"
    );
}
