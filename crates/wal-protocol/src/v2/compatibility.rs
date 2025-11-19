// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Compatibility module with v1. We probably can never drop this
/// code unless we are absolutely sure there is no more records
/// ever exited that are still using v1
use restate_storage_api::deduplication_table::{DedupInformation, EpochSequenceNumber};

use super::{Dedup, Header, IncomingEnvelope, Raw, RecordKind, Source, records};
use crate::v1;

impl TryFrom<super::IncomingEnvelope<Raw>> for v1::Envelope {
    type Error = anyhow::Error;

    fn try_from(value: IncomingEnvelope<Raw>) -> Result<Self, Self::Error> {
        // let Envelope {
        //     header:
        //         Header {
        //             source,
        //             dedup,
        //             kind,
        //         },
        //     mut payload,
        //     ..
        // } = value;

        let (header, _command) = match value.kind() {
            RecordKind::Unknown => anyhow::bail!("Unknown record kind"),
            RecordKind::AnnounceLeader => value
                .into_typed::<records::AnnounceLeader>()
                .split()
                .map(|(header, payload)| (header, v1::Command::AnnounceLeader(payload.into()))),
            RecordKind::VersionBarrier => value
                .into_typed::<records::VersionBarrier>()
                .split()
                .map(|(header, payload)| (header, v1::Command::VersionBarrier(payload))),

            RecordKind::UpdatePartitionDurability => value
                .into_typed::<records::UpdatePartitionDurability>()
                .split()
                .map(|(header, payload)| (header, v1::Command::UpdatePartitionDurability(payload))),
            RecordKind::PatchState => value
                .into_typed::<records::PatchState>()
                .split()
                .map(|(header, payload)| (header, v1::Command::PatchState(payload.into()))),
            RecordKind::TerminateInvocation => value
                .into_typed::<records::TerminateInvocation>()
                .split()
                .map(|(header, payload)| {
                    (header, v1::Command::TerminateInvocation(payload.into()))
                }),
            RecordKind::PurgeInvocation => value
                .into_typed::<records::PurgeInvocation>()
                .split()
                .map(|(header, payload)| (header, v1::Command::PurgeInvocation(payload.into()))),
            RecordKind::PurgeJournal => value
                .into_typed::<records::PurgeJournal>()
                .split()
                .map(|(header, payload)| (header, v1::Command::PurgeJournal(payload.into()))),
            RecordKind::Invoke => value
                .into_typed::<records::Invoke>()
                .split()
                .map(|(header, payload)| (header, v1::Command::Invoke(payload.into()))),
            RecordKind::TruncateOutbox => value
                .into_typed::<records::TruncateOutbox>()
                .split()
                .map(|(header, payload)| (header, v1::Command::TruncateOutbox(payload.into()))),
            RecordKind::ProxyThrough => value
                .into_typed::<records::ProxyThrough>()
                .split()
                .map(|(header, payload)| (header, v1::Command::ProxyThrough(payload.into()))),
            RecordKind::AttachInvocation => value
                .into_typed::<records::AttachInvocation>()
                .split()
                .map(|(header, payload)| (header, v1::Command::AttachInvocation(payload.into()))),
            RecordKind::ResumeInvocation => value
                .into_typed::<records::ResumeInvocation>()
                .split()
                .map(|(header, payload)| (header, v1::Command::ResumeInvocation(payload.into()))),
            RecordKind::RestartAsNewInvocation => value
                .into_typed::<records::RestartAsNewInvocation>()
                .split()
                .map(|(header, payload)| {
                    (header, v1::Command::RestartAsNewInvocation(payload.into()))
                }),
            RecordKind::InvokerEffect => value
                .into_typed::<records::InvokerEffect>()
                .split()
                .map(|(header, payload)| (header, v1::Command::InvokerEffect(payload.into()))),
            RecordKind::Timer => value
                .into_typed::<records::Timer>()
                .split()
                .map(|(header, payload)| (header, v1::Command::Timer(payload.into()))),
            RecordKind::ScheduleTimer => value
                .into_typed::<records::ScheduleTimer>()
                .split()
                .map(|(header, payload)| (header, v1::Command::ScheduleTimer(payload.into()))),
            RecordKind::InvocationResponse => value
                .into_typed::<records::InvocationResponse>()
                .split()
                .map(|(header, payload)| (header, v1::Command::InvocationResponse(payload.into()))),

            RecordKind::NotifyGetInvocationOutputResponse => value
                .into_typed::<records::NotifyGetInvocationOutputResponse>()
                .split()
                .map(|(header, payload)| {
                    (
                        header,
                        v1::Command::NotifyGetInvocationOutputResponse(payload.into()),
                    )
                }),

            RecordKind::NotifySignal => value
                .into_typed::<records::NotifySignal>()
                .split()
                .map(|(header, payload)| (header, v1::Command::NotifySignal(payload.into()))),
            RecordKind::UpsertSchema => value
                .into_typed::<records::UpsertSchema>()
                .split()
                .map(|(header, payload)| (header, v1::Command::UpsertSchema(payload))),
        }?;

        let Header { source, dedup, .. } = header;

        let _source = match source {
            Source::None => anyhow::bail!("Missing envelope header source"),
            Source::Ingress => v1::Source::Ingress {},
            Source::ControlPlane => v1::Source::ControlPlane {},
            Source::Processor {
                partition_id,
                partition_key,
                leader_epoch,
            } => v1::Source::Processor {
                partition_id,
                partition_key,
                leader_epoch,
            },
        };

        let _dedup = match dedup {
            Dedup::None => None,
            Dedup::SelfProposal { leader_epoch, seq } => {
                Some(DedupInformation::self_proposal(EpochSequenceNumber {
                    leader_epoch,
                    sequence_number: seq,
                }))
            }
            Dedup::ForeignPartition { partition, seq } => {
                Some(DedupInformation::cross_partition(partition, seq))
            }
            Dedup::Arbitrary { prefix, seq } => Some(DedupInformation::ingress(prefix, seq)),
        };

        unimplemented!("create `dest` for v1 envelope requires key information")

        // todo: create the dest for v1 envelope will require
        // extra information that can probably by extracted from the
        // keys (which is not part of the envelope)

        // let dest = match destination {
        //     Destination::None => anyhow::bail!("Missing envelope header destination"),
        //     Destination::Processor { partition_key } => v1::Destination::Processor {
        //         partition_key,
        //         dedup,
        //     },
        // };

        // Ok(v1::Envelope::new(v1::Header { source, dest }, command))
    }
}
