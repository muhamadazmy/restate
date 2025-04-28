// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub use encoding_derive::BilrostNewType;
pub use encoding_derive::NetworkMessage;

pub trait NetworkMessage {}

/// A Bilrost compatible U128 type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, BilrostNewType)]
pub struct U128((u64, u64));

impl From<u128> for U128 {
    fn from(value: u128) -> Self {
        Self(((value >> 64) as u64, value as u64))
    }
}

impl From<U128> for u128 {
    fn from(value: U128) -> Self {
        (value.0.0 as u128) << 64 | value.0.1 as u128
    }
}

#[cfg(test)]
mod test {
    use bilrost::{Message, OwnedMessage};
    use encoding_derive::{BilrostNewType, NetworkMessage};
    use rand::random;
    use static_assertions::assert_impl_all;

    use super::U128;

    #[test]
    fn test_u128() {
        (0..100).for_each(|_| {
            let num = random::<u128>();
            let value = U128::from(num);

            assert_eq!(num, u128::from(value));
        });
    }

    #[derive(BilrostNewType)]
    struct MyId(u64);

    #[derive(bilrost::Message)]
    struct Nested {
        id: MyId,
    }

    #[derive(bilrost::Message)]
    struct Flattened {
        id: u64,
    }

    #[test]
    fn test_new_type() {
        let x = Nested { id: MyId(10) };

        let bytes = x.encode_to_bytes();

        let y = Flattened::decode(bytes).expect("decodes");

        assert_eq!(x.id.0, y.id);
    }

    #[derive(NetworkMessage)]
    struct _SomeMessage {}

    assert_impl_all!(_SomeMessage: restate_encoding::NetworkMessage);
}
