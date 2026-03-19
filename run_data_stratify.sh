#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "$0")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
out_dir="${script_dir}/out/data/data_import/preprocessing/data_preprocessing/stratify/data_stratify/default"

rm -f "${out_dir}/data_stratify.train.matrix.tar.gz" \
      "${out_dir}/data_stratify.train.labels.tar.gz" \
      "${out_dir}/data_stratify.test.matrices.tar.gz" \
      "${out_dir}/data_stratify.test.labels.tar.gz" \
      "${out_dir}/data_stratify.label_key.json.gz"

(cd "$repo_root" && python "stratify/data_stratify.py" \
  --name "data_stratify" \
  --output_dir "$out_dir" \
  --data.train_matrix "${repo_root}/preprocessing/out/data/data_import/preprocessing/data_preprocessing/default/data_import.train.matrix.tar.gz" \
  --data.train_labels "${repo_root}/preprocessing/out/data/data_import/preprocessing/data_preprocessing/default/data_import.train.labels.tar.gz" \
  --data.test_matrix "${repo_root}/preprocessing/out/data/data_import/preprocessing/data_preprocessing/default/data_import.test.matrices.tar.gz" \
  --data.true_labels "${repo_root}/preprocessing/out/data/data_import/preprocessing/data_preprocessing/default/data_import.test.labels.tar.gz" \
  --data.label_key "${repo_root}/preprocessing/out/data/data_import/preprocessing/data_preprocessing/default/data_import.label_key.json.gz" \
  "$@")
