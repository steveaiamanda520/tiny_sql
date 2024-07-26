#!/bin/bash

# Define the directory containing the source files
DIRECTORY="/home/jqx/15_445_fall_2023_project/src"

# List of all subdirectories and files to be formatted
FILES=(
    "buffer/buffer_pool_manager.cpp"
    "buffer/lru_k_replacer.cpp"
    "concurrency/watermark.cpp"
    "execution/delete_executor.cpp"
    "container/disk/hash/disk_extendible_hash_table.cpp"
    "execution/aggregation_executor.cpp"
    "common/bustub_ddl.cpp"
    "concurrency/transaction_manager.cpp"
    "execution/execution_common.cpp"
    "execution/filter_executor.cpp"
    "execution/hash_join_executor.cpp"
    "execution/index_scan_executor.cpp"
    "execution/insert_executor.cpp"
    "execution/limit_executor.cpp"
    "execution/nested_loop_join_executor.cpp"
    "execution/seq_scan_executor.cpp"
    "execution/sort_executor.cpp"
    "execution/topn_executor.cpp"
    "execution/topn_per_group_executor.cpp"
    "execution/update_executor.cpp"
    "execution/window_function_executor.cpp"
    "storage/disk/disk_scheduler.cpp"
    "storage/page/extendible_htable_bucket_page.cpp"
    "storage/page/extendible_htable_directory_page.cpp"
    "storage/page/extendible_htable_header_page.cpp"
    "storage/page/page_guard.cpp"
    "optimizer/nlj_as_hash_join.cpp"
    "optimizer/optimizer_custom_rules.cpp"
    "optimizer/optimizer_internal.cpp"
    "optimizer/sort_limit_as_topn.cpp"
    "optimizer/seqscan_as_indexscan.cpp"
)

# Loop through each file and apply clang-format
for file in "${FILES[@]}"; do
    clang-format -style=google -i "$DIRECTORY/$file"
    echo "Formatted: $DIRECTORY/$file"
done

echo "All files have been formatted."

