#!/usr/bin/env bash
set -evx
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
mkdir ${SCRIPT_DIR}/build || true
base=10000000
k=100000
for i in {0..200}
do
    loops=$(expr ${base} \+ ${k} \* ${i})
    sed -i "s/\#define LOOPS .*/\#define LOOPS ${loops}/g" ${SCRIPT_DIR}/main.c
    riscv64-unknown-elf-gcc -nostdlib -nostartfiles -Os ${SCRIPT_DIR}/main.c -o ${SCRIPT_DIR}/build/always_success.${i} -Wl,-static -fdata-sections -ffunction-sections -Wl,--gc-sections -Wl,-s
done
