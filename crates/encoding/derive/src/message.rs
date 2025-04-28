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
use syn::ItemStruct;

pub fn network_message(item: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(item as ItemStruct);

    let name = &input.ident.clone();
    let generics = &input.generics;

    let output = quote! {
        impl ::restate_encoding::NetworkMessage for #name #generics {

        }
    };

    output.into()
}
