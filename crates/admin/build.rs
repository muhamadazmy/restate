// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    tonic_build::configure()
        .bytes(["."])
        .file_descriptor_set_path(out_dir.join("cluster_ctrl_svc_descriptor.bin"))
        .client_mod_attribute("cluster_ctrl", "#[cfg(feature = \"clients\")]")
        // allow older protobuf compiler to be used
        .protoc_arg("--experimental_allow_proto3_optional")
        .extern_path(".restate.common", "::restate_types::protobuf::common")
        .extern_path(".restate.cluster", "::restate_types::protobuf::cluster")
        .extern_path(
            ".restate.deprecated_cluster",
            "::restate_types::protobuf::deprecated_cluster",
        )
        .compile_protos(
            &["./protobuf/cluster_ctrl_svc.proto"],
            &["protobuf", "../types/protobuf"],
        )?;

    Ok(())
}
