// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Context;
use bilrost::{Message, OwnedMessage};
use bytes::{BufMut, Bytes, BytesMut};
use bytestring::ByteString;
use rand::random;
use std::borrow::Cow;

use restate_metadata_store::{ReadError, WriteError};
use restate_types::Version;
use restate_types::config::MetadataClientKind;
use restate_types::metadata::{Precondition, VersionedValue};

use crate::objstore::version_repository::{Tag, ValueEncoding};

use super::version_repository::VersionRepositoryError::PreconditionFailed;
use super::version_repository::{TaggedValue, VersionRepository, VersionRepositoryError};

pub(crate) struct OptimisticLockingMetadataStoreBuilder {
    pub(crate) version_repository: Box<dyn VersionRepository>,
    pub(crate) configuration: MetadataClientKind,
}

impl OptimisticLockingMetadataStoreBuilder {
    pub(crate) async fn build(self) -> anyhow::Result<OptimisticLockingMetadataStore> {
        let MetadataClientKind::ObjectStore { .. } = self.configuration else {
            anyhow::bail!("unexpected configuration value");
        };
        Ok(OptimisticLockingMetadataStore::new(self.version_repository))
    }
}

pub struct OptimisticLockingMetadataStore {
    version_repository: Box<dyn VersionRepository>,
    arena: BytesMut,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(tag = "version", content = "value")]
enum OnDiskValue<'a> {
    V1(Cow<'a, VersionedValue>, u64),
}

fn tagged_value_to_versioned_value(
    tagged_value: TaggedValue,
) -> anyhow::Result<(Tag, VersionedValue)> {
    let tag = tagged_value.tag;
    let versioned_value = match tagged_value.encoding {
        ValueEncoding::Ciborium => {
            let on_disk: OnDiskValue<'static> = ciborium::from_reader(tagged_value.bytes.as_ref())?;
            match on_disk {
                OnDiskValue::V1(cow, _) => Ok(cow.into_owned()),
            }
        }
        ValueEncoding::Bilrost => {
            VersionedValue::decode(tagged_value.bytes).context("failed to decode bilrost")
        }
    }?;
    Ok((tag, versioned_value))
}

impl OptimisticLockingMetadataStore {
    const DEFAULT_ENCODING: ValueEncoding = ValueEncoding::Bilrost;

    fn new(version_repository: Box<dyn VersionRepository>) -> Self {
        Self {
            version_repository,
            arena: BytesMut::with_capacity(8196),
        }
    }

    pub(crate) async fn get(
        &mut self,
        key: ByteString,
    ) -> Result<Option<VersionedValue>, ReadError> {
        match self.version_repository.get(key).await {
            Ok(res) => {
                let (_, d) =
                    tagged_value_to_versioned_value(res).map_err(|e| ReadError::Codec(e.into()))?;
                Ok(Some(d))
            }
            Err(VersionRepositoryError::NotFound) => Ok(None),
            Err(e) => Err(ReadError::retryable(e)),
        }
    }

    pub(crate) async fn get_version(
        &mut self,
        key: ByteString,
    ) -> Result<Option<Version>, ReadError> {
        if let Some(res) = self.get(key).await? {
            Ok(Some(res.version))
        } else {
            Ok(None)
        }
    }

    fn serialize_versioned_value(
        &mut self,
        encoding: ValueEncoding,
        versioned_value: &VersionedValue,
        cookie: u64,
    ) -> Result<Bytes, WriteError> {
        match encoding {
            ValueEncoding::Ciborium => {
                self.arena.clear();
                let writer = (&mut self.arena).writer();
                let on_disk = OnDiskValue::V1(Cow::Borrowed(versioned_value), cookie);
                ciborium::into_writer(&on_disk, writer)
                    .map(|_| self.arena.split().freeze())
                    .map_err(|e| WriteError::Codec(e.into()))
            }
            ValueEncoding::Bilrost => {
                self.arena.clear();
                versioned_value
                    .encode(&mut self.arena)
                    .map_err(|err| WriteError::Codec(err.into()))?;
                Ok(self.arena.split().freeze())
            }
        }
    }

    pub(crate) async fn put(
        &mut self,
        key: ByteString,
        value: VersionedValue,
        precondition: Precondition,
    ) -> Result<(), WriteError> {
        // create a random cookie
        let cookie = random::<u64>();
        let buf = self.serialize_versioned_value(Self::DEFAULT_ENCODING, &value, cookie)?;
        match precondition {
            Precondition::None => {
                self.version_repository
                    .put(key, Self::DEFAULT_ENCODING, buf)
                    .await
                    .map_err(WriteError::retryable)?;
                Ok(())
            }
            Precondition::DoesNotExist => match self
                .version_repository
                .create(key, Self::DEFAULT_ENCODING, buf)
                .await
            {
                Ok(_) => Ok(()),
                Err(VersionRepositoryError::AlreadyExists) => {
                    Err(WriteError::FailedPrecondition("already exists".to_string()))
                }
                Err(e) => Err(WriteError::retryable(e)),
            },
            Precondition::MatchesVersion(version) => {
                // we need to get the current version here, because the version provided by the API does not
                // match the version provided by the object store (ETag vs logical version)
                //
                // 1. get the current logical version and the object store tag.
                //
                let (current_tag, current_version) =
                    match self.version_repository.get(key.clone()).await {
                        Ok(tagged) => {
                            let (tag, versioned_value) = tagged_value_to_versioned_value(tagged)
                                .map_err(|e| WriteError::Codec(e.into()))?;
                            (tag, versioned_value.version)
                        }
                        Err(VersionRepositoryError::NotFound) => {
                            return Err(WriteError::FailedPrecondition(
                                "no current version exists".to_string(),
                            ));
                        }
                        Err(e) => return Err(WriteError::retryable(e)),
                    };
                //
                // 2. check if logical version is the expected version
                //
                if current_version != version {
                    return Err(WriteError::FailedPrecondition(format!(
                        "expected {} != got {}",
                        version, current_version
                    )));
                }
                //
                // 3. try compare and set
                //
                match self
                    .version_repository
                    .put_if_tag_matches(key, current_tag, Self::DEFAULT_ENCODING, buf)
                    .await
                {
                    Ok(_) => Ok(()),
                    Err(PreconditionFailed) => Err(WriteError::FailedPrecondition(
                        "failed precondition".to_string(),
                    )),
                    Err(e) => Err(WriteError::retryable(e)),
                }
            }
        }
    }

    pub(crate) async fn delete(
        &mut self,
        key: ByteString,
        precondition: Precondition,
    ) -> Result<(), WriteError> {
        match precondition {
            Precondition::None => match self.version_repository.delete(key).await {
                Ok(_) => Ok(()),
                Err(e) => Err(WriteError::retryable(e)),
            },
            Precondition::DoesNotExist => Err(WriteError::terminal(NonSensicalPrecondition)),
            Precondition::MatchesVersion(version) => {
                // we need to convert a version into a tag, this mean we need to do a read first.
                let (tag, current_version) = match self.version_repository.get(key.clone()).await {
                    Ok(res) => {
                        let (tag, versioned_value) = tagged_value_to_versioned_value(res)
                            .map_err(|e| WriteError::Codec(e.into()))?;
                        (tag, versioned_value.version)
                    }
                    Err(VersionRepositoryError::NotFound) => {
                        return Err(WriteError::FailedPrecondition(
                            "No version found".to_string(),
                        ));
                    }
                    Err(e) => return Err(WriteError::retryable(e)),
                };

                if current_version != version {
                    return Err(WriteError::FailedPrecondition(
                        "version mismatch".to_string(),
                    ));
                }

                match self
                    .version_repository
                    .delete_if_tag_matches(key, tag)
                    .await
                {
                    Ok(_) => Ok(()),
                    Err(PreconditionFailed) => Err(WriteError::FailedPrecondition(
                        "failed precondition".to_string(),
                    )),
                    Err(e) => Err(WriteError::retryable(e)),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use bytestring::ByteString;
    use restate_metadata_store::WriteError;
    use restate_types::Version;
    use restate_types::metadata::{Precondition, VersionedValue};

    use crate::objstore::object_store_version_repository::ObjectStoreVersionRepository;
    use crate::objstore::optimistic_store::{
        OptimisticLockingMetadataStore, tagged_value_to_versioned_value,
    };
    use crate::objstore::version_repository::{Tag, TaggedValue, ValueEncoding};

    const KEY_1: ByteString = ByteString::from_static("1");
    const HELLO: Bytes = Bytes::from_static(b"hello");

    #[tokio::test]
    async fn basic_example() {
        let mut store = OptimisticLockingMetadataStore::new(Box::new(
            ObjectStoreVersionRepository::new_for_testing(),
        ));

        store
            .put(
                KEY_1,
                VersionedValue::new(Version::MIN.next(), HELLO),
                Precondition::None,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn put_if_absent() {
        let mut store = OptimisticLockingMetadataStore::new(Box::new(
            ObjectStoreVersionRepository::new_for_testing(),
        ));

        store
            .put(
                KEY_1,
                VersionedValue::new(Version::MIN.next(), HELLO),
                Precondition::DoesNotExist,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn put_if_absent_should_fail() {
        let mut store = OptimisticLockingMetadataStore::new(Box::new(
            ObjectStoreVersionRepository::new_for_testing(),
        ));

        store
            .put(
                KEY_1,
                VersionedValue::new(Version::MIN.next(), HELLO),
                Precondition::DoesNotExist,
            )
            .await
            .unwrap();

        match store
            .put(
                KEY_1,
                VersionedValue::new(Version::MIN.next(), HELLO),
                Precondition::DoesNotExist,
            )
            .await
        {
            Err(WriteError::FailedPrecondition(_)) => {
                // ok
            }
            _ => {
                panic!("Expected WriteError::FailedPrecondition");
            }
        }
    }

    #[tokio::test]
    async fn put_if_absent_on_deleted_value() {
        let mut store = OptimisticLockingMetadataStore::new(Box::new(
            ObjectStoreVersionRepository::new_for_testing(),
        ));

        store
            .put(
                KEY_1,
                VersionedValue::new(Version::MIN.next(), HELLO),
                Precondition::DoesNotExist,
            )
            .await
            .unwrap();
        store.delete(KEY_1, Precondition::None).await.unwrap();

        store
            .put(
                KEY_1,
                VersionedValue::new(Version::MIN.next(), HELLO),
                Precondition::DoesNotExist,
            )
            .await
            .unwrap();
    }

    #[test]
    fn test_encoding() {
        let mut store = OptimisticLockingMetadataStore::new(Box::new(
            ObjectStoreVersionRepository::new_for_testing(),
        ));

        let value = VersionedValue::new(Version::MIN.next(), HELLO);

        let buf = store
            .serialize_versioned_value(ValueEncoding::Ciborium, &value, 0)
            .unwrap();

        let tagged_value = TaggedValue {
            tag: Tag::from("test".to_string()),
            encoding: ValueEncoding::Ciborium,
            bytes: buf,
        };

        let (tag, versioned_value) = tagged_value_to_versioned_value(tagged_value).unwrap();
        assert_eq!(tag, Tag::from("test".to_string()));
        assert_eq!(versioned_value.value, HELLO);

        let buf = store
            .serialize_versioned_value(ValueEncoding::Bilrost, &value, 0)
            .unwrap();

        let tagged_value = TaggedValue {
            tag: Tag::from("test".to_string()),
            encoding: ValueEncoding::Bilrost,
            bytes: buf,
        };

        let (tag, versioned_value) = tagged_value_to_versioned_value(tagged_value).unwrap();
        assert_eq!(tag, Tag::from("test".to_string()));
        assert_eq!(versioned_value.value, HELLO);
    }
}

#[derive(Debug, thiserror::Error)]
#[error("This combination does not make sense")]
struct NonSensicalPrecondition;
