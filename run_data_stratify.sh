#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "$0")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
out_dir="${script_dir}/out/data/data_import/preprocessing/data_preprocessing/stratify/data_stratify/default"
compat_dir="${out_dir}/compat_data_import"
dataset_name="${1:-FR-FCM-Z2KP-covid}"

if [[ $# -gt 0 ]]; then
  shift
fi

rm -f "${out_dir}/data_stratify.train.matrix.tar.gz" \
      "${out_dir}/data_stratify.train.labels.tar.gz" \
      "${out_dir}/data_stratify.test.matrices.tar.gz" \
      "${out_dir}/data_stratify.test.labels.tar.gz" \
      "${out_dir}/data_stratify.metadata.json.gz"

(cd "$repo_root" && python "stratify/data_stratify.py" \
  --name "data_stratify" \
  --output_dir "$out_dir" \
  --data.train_matrix "${repo_root}/preprocessing/out/data/data_import/preprocessing/data_preprocessing/default/data_import.train.matrix.tar.gz" \
  --data.train_labels "${repo_root}/preprocessing/out/data/data_import/preprocessing/data_preprocessing/default/data_import.train.labels.tar.gz" \
  --data.test_matrix "${repo_root}/preprocessing/out/data/data_import/preprocessing/data_preprocessing/default/data_import.test.matrices.tar.gz" \
  --data.true_labels "${repo_root}/preprocessing/out/data/data_import/preprocessing/data_preprocessing/default/data_import.test.labels.tar.gz" \
  --data.metadata "${repo_root}/preprocessing/out/data/data_import/preprocessing/data_preprocessing/default/data_import.metadata.json.gz" \
  "$@")

mkdir -p "$compat_dir"
ln -sfn "${out_dir}/data_stratify.train.matrix.tar.gz" "$compat_dir/data_import.train.matrix.tar.gz"
ln -sfn "${out_dir}/data_stratify.train.labels.tar.gz" "$compat_dir/data_import.train.labels.tar.gz"
ln -sfn "${out_dir}/data_stratify.test.matrices.tar.gz" "$compat_dir/data_import.test.matrices.tar.gz"
ln -sfn "${out_dir}/data_stratify.test.labels.tar.gz" "$compat_dir/data_import.test.labels.tar.gz"
ln -sfn "${out_dir}/data_stratify.metadata.json.gz" "$compat_dir/data_import.metadata.json.gz"

for model in dgcytof gatemeclass deepcytof CyGATE cyanno LDA random knn; do
  model_out_dir="${repo_root}/models/${model}/out/data/data_preprocessing/default"
  mkdir -p "$(dirname "$model_out_dir")"
  if [[ -e "$model_out_dir" && ! -L "$model_out_dir" ]]; then
    rm -rf "$model_out_dir"
  fi
  ln -sfn "$compat_dir" "$model_out_dir"
done
