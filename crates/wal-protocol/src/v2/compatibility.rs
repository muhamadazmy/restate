// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::deduplication_table::{
    DedupInformation, DedupSequenceNumber, EpochSequenceNumber, ProducerId,
};
use restate_types::logs::{HasRecordKeys, Keys};

use super::{Raw, records};
use crate::{
    v1,
    v2::{
        self,
        records::{ProxyThroughRequest, TruncateOutboxRequest},
    },
};

impl TryFrom<v1::Envelope> for v2::IncomingEnvelope<Raw> {
    type Error = anyhow::Error;

    fn try_from(value: v1::Envelope) -> Result<Self, Self::Error> {
        let source = match value.header.source {
            v1::Source::Ingress {} => v2::Source::Ingress,
            v1::Source::Processor {
                partition_id,
                partition_key,
                leader_epoch,
            } => v2::Source::Processor {
                partition_id,
                partition_key,
                leader_epoch,
            },
            v1::Source::ControlPlane {} => v2::Source::ControlPlane,
        };

        let v1::Destination::Processor {
            dedup,
            partition_key,
        } = value.header.dest;

        let dedup = match dedup {
            None => v2::Dedup::None,
            Some(info) => match (info.producer_id, info.sequence_number) {
                (ProducerId::Partition(id), DedupSequenceNumber::Sn(seq)) => {
                    v2::Dedup::ForeignPartition { partition: id, seq }
                }
                (ProducerId::Partition(_), _) => anyhow::bail!("invalid deduplication information"),
                (ProducerId::Other(_), DedupSequenceNumber::Esn(sn)) => v2::Dedup::SelfProposal {
                    leader_epoch: sn.leader_epoch,
                    seq: sn.sequence_number,
                },
                (ProducerId::Other(prefix), DedupSequenceNumber::Sn(seq)) => v2::Dedup::Arbitrary {
                    prefix: prefix.into(),
                    seq,
                },
            },
        };

        let envelope = match value.command {
            v1::Command::AnnounceLeader(payload) => {
                Self::from_parts::<records::AnnounceLeader>(source, dedup, *payload)
            }
            v1::Command::AttachInvocation(payload) => {
                Self::from_parts::<records::AttachInvocation>(source, dedup, payload.into())
            }
            v1::Command::InvocationResponse(payload) => {
                Self::from_parts::<records::InvocationResponse>(source, dedup, payload.into())
            }
            v1::Command::Invoke(payload) => {
                Self::from_parts::<records::Invoke>(source, dedup, payload.into())
            }
            v1::Command::InvokerEffect(payload) => {
                Self::from_parts::<records::InvokerEffect>(source, dedup, payload.into())
            }
            v1::Command::NotifyGetInvocationOutputResponse(payload) => {
                Self::from_parts::<records::NotifyGetInvocationOutputResponse>(
                    source,
                    dedup,
                    payload.into(),
                )
            }
            v1::Command::NotifySignal(payload) => {
                Self::from_parts::<records::NotifySignal>(source, dedup, payload.into())
            }
            v1::Command::PatchState(payload) => {
                Self::from_parts::<records::PatchState>(source, dedup, payload.into())
            }
            v1::Command::ProxyThrough(payload) => Self::from_parts::<records::ProxyThrough>(
                source,
                dedup,
                ProxyThroughRequest {
                    invocation: payload.into(),
                    proxy_partition: Keys::Single(partition_key),
                },
            ),
            v1::Command::PurgeInvocation(payload) => {
                Self::from_parts::<records::PurgeInvocation>(source, dedup, payload.into())
            }
            v1::Command::PurgeJournal(payload) => {
                Self::from_parts::<records::PurgeJournal>(source, dedup, payload.into())
            }
            v1::Command::RestartAsNewInvocation(payload) => {
                Self::from_parts::<records::RestartAsNewInvocation>(source, dedup, payload.into())
            }
            v1::Command::ResumeInvocation(payload) => {
                Self::from_parts::<records::ResumeInvocation>(source, dedup, payload.into())
            }
            v1::Command::ScheduleTimer(payload) => {
                Self::from_parts::<records::ScheduleTimer>(source, dedup, payload.into())
            }
            v1::Command::TerminateInvocation(payload) => {
                Self::from_parts::<records::TerminateInvocation>(source, dedup, payload.into())
            }
            v1::Command::Timer(payload) => {
                Self::from_parts::<records::Timer>(source, dedup, payload.into())
            }
            v1::Command::TruncateOutbox(payload) => Self::from_parts::<records::TruncateOutbox>(
                source,
                dedup,
                TruncateOutboxRequest {
                    index: payload,
                    // this actually should be a key-range but v1 unfortunately
                    // only hold the "start" of the range.
                    // will be fixed in v2
                    partition_key_range: Keys::Single(partition_key),
                },
            ),
            v1::Command::UpdatePartitionDurability(payload) => {
                Self::from_parts::<records::UpdatePartitionDurability>(source, dedup, payload)
            }
            v1::Command::UpsertSchema(payload) => {
                Self::from_parts::<records::UpsertSchema>(source, dedup, payload)
            }
            v1::Command::VersionBarrier(payload) => {
                Self::from_parts::<records::VersionBarrier>(source, dedup, payload)
            }
        };

        Ok(envelope)
    }
}

impl From<(Keys, v2::Header)> for v1::Header {
    fn from((keys, value): (Keys, v2::Header)) -> Self {
        let source = match value.source {
            v2::Source::Ingress => v1::Source::Ingress {},
            v2::Source::ControlPlane => v1::Source::ControlPlane {},
            v2::Source::Processor {
                partition_id,
                partition_key,
                leader_epoch,
            } => v1::Source::Processor {
                partition_id,
                partition_key,
                leader_epoch,
            },
        };

        // this is only for backward compatibility
        // but in reality the partition_id in the dest
        // should never be used. Instead the associated
        // record keys should.
        let partition_key = match keys {
            Keys::None => 0,
            Keys::Single(pk) => pk,
            Keys::Pair(pk, _) => pk,
            Keys::RangeInclusive(range) => *range.start(),
        };

        let dedup = match value.dedup {
            v2::Dedup::None => None,
            v2::Dedup::SelfProposal { leader_epoch, seq } => {
                Some(DedupInformation::self_proposal(EpochSequenceNumber {
                    leader_epoch,
                    sequence_number: seq,
                }))
            }
            v2::Dedup::ForeignPartition { partition, seq } => {
                Some(DedupInformation::cross_partition(partition, seq))
            }
            v2::Dedup::Arbitrary { prefix, seq } => Some(DedupInformation::ingress(prefix, seq)),
        };

        v1::Header {
            source,
            dest: v1::Destination::Processor {
                partition_key,
                dedup,
            },
        }
    }
}

// compatibility from v2 to v1. We will keep writing v1 envelops
// until the following release.

impl From<v2::Envelope<records::AnnounceLeader>> for v1::Envelope {
    fn from(value: v2::Envelope<records::AnnounceLeader>) -> Self {
        let header = v1::Header::from((value.record_keys(), value.header));

        Self {
            header,
            command: v1::Command::AnnounceLeader(value.payload.into()),
        }
    }
}

impl From<v2::Envelope<records::VersionBarrier>> for v1::Envelope {
    fn from(value: v2::Envelope<records::VersionBarrier>) -> Self {
        let header = v1::Header::from((value.record_keys(), value.header));

        Self {
            header,
            command: v1::Command::VersionBarrier(value.payload),
        }
    }
}

impl From<v2::Envelope<records::UpdatePartitionDurability>> for v1::Envelope {
    fn from(value: v2::Envelope<records::UpdatePartitionDurability>) -> Self {
        let header = v1::Header::from((value.record_keys(), value.header));

        Self {
            header,
            command: v1::Command::UpdatePartitionDurability(value.payload),
        }
    }
}

impl From<v2::Envelope<records::PatchState>> for v1::Envelope {
    fn from(value: v2::Envelope<records::PatchState>) -> Self {
        let header = v1::Header::from((value.record_keys(), value.header));

        Self {
            header,
            command: v1::Command::PatchState(value.payload.into()),
        }
    }
}

impl From<v2::Envelope<records::TerminateInvocation>> for v1::Envelope {
    fn from(value: v2::Envelope<records::TerminateInvocation>) -> Self {
        let header = v1::Header::from((value.record_keys(), value.header));

        Self {
            header,
            command: v1::Command::TerminateInvocation(value.payload.into()),
        }
    }
}

impl From<v2::Envelope<records::PurgeInvocation>> for v1::Envelope {
    fn from(value: v2::Envelope<records::PurgeInvocation>) -> Self {
        let header = v1::Header::from((value.record_keys(), value.header));

        Self {
            header,
            command: v1::Command::PurgeInvocation(value.payload.into()),
        }
    }
}

impl From<v2::Envelope<records::PurgeJournal>> for v1::Envelope {
    fn from(value: v2::Envelope<records::PurgeJournal>) -> Self {
        let header = v1::Header::from((value.record_keys(), value.header));

        Self {
            header,
            command: v1::Command::PurgeJournal(value.payload.into()),
        }
    }
}

impl From<v2::Envelope<records::Invoke>> for v1::Envelope {
    fn from(value: v2::Envelope<records::Invoke>) -> Self {
        let header = v1::Header::from((value.record_keys(), value.header));

        Self {
            header,
            command: v1::Command::Invoke(value.payload.into()),
        }
    }
}

impl From<v2::Envelope<records::TruncateOutbox>> for v1::Envelope {
    fn from(value: v2::Envelope<records::TruncateOutbox>) -> Self {
        let header = v1::Header::from((value.record_keys(), value.header));

        Self {
            header,
            command: v1::Command::TruncateOutbox(value.payload.index),
        }
    }
}

impl From<v2::Envelope<records::ProxyThrough>> for v1::Envelope {
    fn from(value: v2::Envelope<records::ProxyThrough>) -> Self {
        let header = v1::Header::from((value.record_keys(), value.header));

        Self {
            header,
            command: v1::Command::ProxyThrough(value.payload.invocation.into()),
        }
    }
}

impl From<v2::Envelope<records::AttachInvocation>> for v1::Envelope {
    fn from(value: v2::Envelope<records::AttachInvocation>) -> Self {
        let header = v1::Header::from((value.record_keys(), value.header));

        Self {
            header,
            command: v1::Command::AttachInvocation(value.payload.into()),
        }
    }
}

impl From<v2::Envelope<records::ResumeInvocation>> for v1::Envelope {
    fn from(value: v2::Envelope<records::ResumeInvocation>) -> Self {
        let header = v1::Header::from((value.record_keys(), value.header));

        Self {
            header,
            command: v1::Command::ResumeInvocation(value.payload.into()),
        }
    }
}

impl From<v2::Envelope<records::RestartAsNewInvocation>> for v1::Envelope {
    fn from(value: v2::Envelope<records::RestartAsNewInvocation>) -> Self {
        let header = v1::Header::from((value.record_keys(), value.header));

        Self {
            header,
            command: v1::Command::RestartAsNewInvocation(value.payload.into()),
        }
    }
}

impl From<v2::Envelope<records::InvokerEffect>> for v1::Envelope {
    fn from(value: v2::Envelope<records::InvokerEffect>) -> Self {
        let header = v1::Header::from((value.record_keys(), value.header));

        Self {
            header,
            command: v1::Command::InvokerEffect(value.payload.into()),
        }
    }
}

impl From<v2::Envelope<records::Timer>> for v1::Envelope {
    fn from(value: v2::Envelope<records::Timer>) -> Self {
        let header = v1::Header::from((value.record_keys(), value.header));

        Self {
            header,
            command: v1::Command::Timer(value.payload.into()),
        }
    }
}

impl From<v2::Envelope<records::ScheduleTimer>> for v1::Envelope {
    fn from(value: v2::Envelope<records::ScheduleTimer>) -> Self {
        let header = v1::Header::from((value.record_keys(), value.header));

        Self {
            header,
            command: v1::Command::ScheduleTimer(value.payload.into()),
        }
    }
}

impl From<v2::Envelope<records::InvocationResponse>> for v1::Envelope {
    fn from(value: v2::Envelope<records::InvocationResponse>) -> Self {
        let header = v1::Header::from((value.record_keys(), value.header));

        Self {
            header,
            command: v1::Command::InvocationResponse(value.payload.into()),
        }
    }
}

impl From<v2::Envelope<records::NotifyGetInvocationOutputResponse>> for v1::Envelope {
    fn from(value: v2::Envelope<records::NotifyGetInvocationOutputResponse>) -> Self {
        let header = v1::Header::from((value.record_keys(), value.header));

        Self {
            header,
            command: v1::Command::NotifyGetInvocationOutputResponse(value.payload.into()),
        }
    }
}

impl From<v2::Envelope<records::NotifySignal>> for v1::Envelope {
    fn from(value: v2::Envelope<records::NotifySignal>) -> Self {
        let header = v1::Header::from((value.record_keys(), value.header));

        Self {
            header,
            command: v1::Command::NotifySignal(value.payload.into()),
        }
    }
}

impl From<v2::Envelope<records::UpsertSchema>> for v1::Envelope {
    fn from(value: v2::Envelope<records::UpsertSchema>) -> Self {
        let header = v1::Header::from((value.record_keys(), value.header));

        Self {
            header,
            command: v1::Command::UpsertSchema(value.payload),
        }
    }
}
