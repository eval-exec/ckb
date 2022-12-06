#!/usr/bin/env bash
./test-rayon-nest/compile.sh
set -evx
cargo test --package ckb-script --lib verify::tests::ckb_2021::features_since_v2019::rayon_nest_verify -- --exact --nocapture
