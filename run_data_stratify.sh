#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "$0")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
dataset_name="${1:-FR-FCM-Z2KP-covid}"
if [[ $# -gt 0 ]]; then
  shift
fi

out_dir="${script_dir}/out/data/data_import/stratify/data_stratify/default"
rm -f "${out_dir}/data_stratify.data.tar.gz" "${out_dir}/data_stratify.order.json.gz"

(cd "$repo_root" && python "stratify/data_stratify.py" \
  --name "data_stratify" \
  --output_dir "$out_dir" \
  --data.raw "${repo_root}/data/out/data/data_import/${dataset_name}.data.tar.gz" \
  --data.order "${repo_root}/data/out/data/data_import/${dataset_name}.order.json.gz" \
  "$@")
