# Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
# All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This docker file is mainly used to build a local docker image
# with restate binary to run sdk tests

FROM archlinux/archlinux AS runtime
RUN pacman -Syu --noconfirm
COPY ./target/debug/restate-server /usr/local/bin/
WORKDIR /
ENTRYPOINT ["/usr/local/bin/restate-server"]