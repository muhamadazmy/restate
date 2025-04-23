// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use proc_macro::TokenStream;
use quote::quote;
use syn::{Fields, Ident, ItemStruct};

pub fn new_type(item: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(item as ItemStruct);
    let inner = match &input.fields {
        Fields::Unnamed(inner) => {
            if inner.unnamed.len() != 1 {
                return syn::Error::new_spanned(
                    input.ident,
                    "This macro can only be used on newtype struct with exactly one field",
                )
                .to_compile_error()
                .into();
            }

            &inner.unnamed[0]
        }
        _ => {
            return syn::Error::new_spanned(
                input.ident,
                "This macro can only be used on newtype structs (e.g., `struct MyType(T);`)",
            )
            .to_compile_error()
            .into();
        }
    };

    let name = &input.ident;
    let inner_ty = &inner.ty;
    let mod_name = Ident::new(&format!("{}_flatten", name), name.span());

    let output = quote! {

        #[allow(non_snake_case)]
        mod #mod_name {
            use super::#name;
            use ::bilrost::encoding::{
                EmptyState, ForOverwrite, General, ValueDecoder, ValueEncoder, Wiretyped,
            };

            impl ValueEncoder<General> for #name {
                fn encode_value<B: bytes::BufMut + ?Sized>(value: &Self, buf: &mut B) {
                    <#inner_ty as ValueEncoder<General>>::encode_value(&value.0, buf)
                }

                fn value_encoded_len(value: &Self) -> usize {
                    <#inner_ty as ValueEncoder<General>>::value_encoded_len(&value.0)
                }

                fn prepend_value<B: bilrost::buf::ReverseBuf + ?Sized>(value: &Self, buf: &mut B) {
                    <#inner_ty as ValueEncoder<General>>::prepend_value(&value.0, buf);
                }
            }

            impl ValueDecoder<General> for #name {
                fn decode_value<B: bytes::Buf + ?Sized>(
                    value: &mut Self,
                    buf: bilrost::encoding::Capped<B>,
                    ctx: bilrost::encoding::DecodeContext,
                ) -> Result<(), bilrost::DecodeError> {
                    <#inner_ty as ValueDecoder<General>>::decode_value(&mut value.0, buf, ctx)
                }
            }

            impl Wiretyped<General> for #name {
                const WIRE_TYPE: bilrost::encoding::WireType = <#inner_ty as Wiretyped<General>>::WIRE_TYPE;
            }

            impl EmptyState for #name {
                fn clear(&mut self) {
                    <#inner_ty as EmptyState>::clear(&mut self.0);
                }
                fn empty() -> Self
                where
                    Self: Sized,
                {
                    Self(<#inner_ty as EmptyState>::empty())
                }
                fn is_empty(&self) -> bool {
                    <#inner_ty as EmptyState>::is_empty(&self.0)
                }
            }

            impl ForOverwrite for #name {
                fn for_overwrite() -> Self
                where
                    Self: Sized,
                {
                    Self(<#inner_ty as ForOverwrite>::for_overwrite())
                }
            }
        }
    };

    output.into()
}
