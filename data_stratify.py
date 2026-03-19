#!/usr/bin/env python3

"""Filter preprocessed train/test archives in a stratify stage."""

from __future__ import annotations

import argparse
import gzip
import io
import shutil
import tarfile
import tempfile
from pathlib import Path
from typing import Iterable


TAR_GZIP_COMPRESSLEVEL = 1


def is_tar_archive(path: Path) -> bool:
    if not path.is_file():
        return False
    try:
        return tarfile.is_tarfile(path)
    except (OSError, tarfile.TarError):
        return False


def parse_bool(value: str) -> bool:
    lowered = value.strip().lower()
    if lowered in {'1', 'true', 'yes', 'y', 'on'}:
        return True
    if lowered in {'0', 'false', 'no', 'n', 'off'}:
        return False
    raise argparse.ArgumentTypeError(f'Invalid boolean value: {value}')


def label_is_zero(text: str) -> bool:
    stripped = text.strip()
    if not stripped:
        return False
    try:
        return float(stripped) == 0.0
    except ValueError:
        return False


def sorted_csv_members(archive_path: Path) -> list[tarfile.TarInfo]:
    with tarfile.open(archive_path, mode='r:*') as archive:
        members = [member for member in archive.getmembers() if member.isfile()]
    csv_members = [member for member in members if member.name.lower().endswith('.csv')]
    if not csv_members:
        raise ValueError(f'No CSV members found in archive: {archive_path}')
    return sorted(csv_members, key=lambda member: member.name)


def sample_key(member_name: str) -> str:
    stem = Path(member_name).stem
    if '-data-' in stem:
        return stem.rsplit('-data-', 1)[1]
    if '-label-' in stem:
        return stem.rsplit('-label-', 1)[1]
    return stem


def extract_archive_members(archive_path: Path, destination: Path) -> list[Path]:
    destination.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive_path, mode='r:*') as archive:
        members = [member for member in archive.getmembers() if member.isfile()]
        for member in members:
            archive.extract(member, path=destination, filter='data')
    extracted = [path for path in destination.rglob('*') if path.is_file()]
    if not extracted:
        raise ValueError(f'No files extracted from archive: {archive_path}')
    return sorted(extracted, key=lambda path: path.name)


def filter_matrix_and_labels(
    matrix_path: Path, labels_path: Path, destination_dir: Path
) -> tuple[Path, Path]:
    destination_dir.mkdir(parents=True, exist_ok=True)
    filtered_matrix_path = destination_dir / matrix_path.name
    filtered_labels_path = destination_dir / labels_path.name

    kept_rows = 0
    with open(matrix_path, 'r', encoding='utf-8', newline='') as matrix_handle, open(
        labels_path, 'r', encoding='utf-8', newline=''
    ) as labels_handle, open(filtered_matrix_path, 'w', encoding='utf-8', newline='') as out_matrix, open(
        filtered_labels_path, 'w', encoding='utf-8', newline=''
    ) as out_labels:
        while True:
            matrix_line = matrix_handle.readline()
            label_line = labels_handle.readline()

            if not matrix_line and not label_line:
                break
            if not matrix_line or not label_line:
                raise ValueError(
                    f'Row count mismatch between {matrix_path.name} and {labels_path.name}.'
                )

            label_value = label_line.rstrip('\r\n')
            if label_is_zero(label_value):
                continue

            out_matrix.write(matrix_line)
            out_labels.write(label_line)
            kept_rows += 1

    if kept_rows < 0:
        raise ValueError('Unexpected negative kept row count.')

    return filtered_matrix_path, filtered_labels_path


def write_tar_from_paths(paths: Iterable[Path], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(output_path, mode='w:gz', compresslevel=TAR_GZIP_COMPRESSLEVEL) as archive:
        wrote_file = False
        for path in paths:
            archive.add(path, arcname=path.name)
            wrote_file = True
    if not wrote_file:
        raise ValueError(f'No files written to archive: {output_path}')


def copy_if_needed(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)


def stratify_training(
    train_matrix: Path,
    train_labels: Path,
    output_dir: Path,
    name: str,
    drop_ungated_training: bool,
) -> None:
    if not drop_ungated_training:
        copy_if_needed(train_matrix, output_dir / f'{name}.train.matrix.tar.gz')
        copy_if_needed(train_labels, output_dir / f'{name}.train.labels.tar.gz')
        return

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        matrix_paths = extract_archive_members(train_matrix, tmp_path / 'train_matrix')
        label_paths = extract_archive_members(train_labels, tmp_path / 'train_labels')
        if len(matrix_paths) != 1 or len(label_paths) != 1:
            raise ValueError('Training archives must each contain exactly one CSV file.')

        filtered_matrix, filtered_labels = filter_matrix_and_labels(
            matrix_paths[0], label_paths[0], tmp_path / 'train_filtered'
        )
        write_tar_from_paths([filtered_matrix], output_dir / f'{name}.train.matrix.tar.gz')
        write_tar_from_paths([filtered_labels], output_dir / f'{name}.train.labels.tar.gz')


def stratify_test(
    test_matrix: Path,
    true_labels: Path,
    output_dir: Path,
    name: str,
    drop_ungated_test: bool,
) -> None:
    if not drop_ungated_test:
        copy_if_needed(test_matrix, output_dir / f'{name}.test.matrices.tar.gz')
        copy_if_needed(true_labels, output_dir / f'{name}.test.labels.tar.gz')
        return

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        matrix_paths = extract_archive_members(test_matrix, tmp_path / 'test_matrix')
        label_paths = extract_archive_members(true_labels, tmp_path / 'true_labels')

        matrix_by_key = {sample_key(path.name): path for path in matrix_paths}
        label_by_key = {sample_key(path.name): path for path in label_paths}
        if set(matrix_by_key) != set(label_by_key):
            raise ValueError('Test matrix and label archives must contain matching sample ids.')

        filtered_matrix_paths: list[Path] = []
        filtered_label_paths: list[Path] = []
        for key in sorted(matrix_by_key):
            filtered_matrix, filtered_labels = filter_matrix_and_labels(
                matrix_by_key[key], label_by_key[key], tmp_path / 'test_filtered'
            )
            filtered_matrix_paths.append(filtered_matrix)
            filtered_label_paths.append(filtered_labels)

        write_tar_from_paths(filtered_matrix_paths, output_dir / f'{name}.test.matrices.tar.gz')
        write_tar_from_paths(filtered_label_paths, output_dir / f'{name}.test.labels.tar.gz')


def parse_args() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data.train_matrix', dest='data.train_matrix', required=True)
    parser.add_argument('--data.train_labels', dest='data.train_labels', required=True)
    parser.add_argument('--data.test_matrix', dest='data.test_matrix', required=True)
    parser.add_argument('--data.true_labels', dest='data.true_labels', required=True)
    parser.add_argument('--data.label_key', dest='data.label_key', required=True)
    parser.add_argument('--output_dir', required=True)
    parser.add_argument('--name', default='data_stratify')
    parser.add_argument(
        '--drop-ungated-training',
        type=parse_bool,
        default=False,
        help='Drop training rows where the label equals 0.',
    )
    parser.add_argument(
        '--drop-ungated-test',
        type=parse_bool,
        default=False,
        help='Drop test rows where the label equals 0.',
    )
    return parser


def main() -> None:
    args = parse_args().parse_args()

    train_matrix = Path(getattr(args, 'data.train_matrix'))
    train_labels = Path(getattr(args, 'data.train_labels'))
    test_matrix = Path(getattr(args, 'data.test_matrix'))
    true_labels = Path(getattr(args, 'data.true_labels'))
    label_key = Path(getattr(args, 'data.label_key'))
    output_dir = Path(args.output_dir)
    name = args.name

    for archive_path in (train_matrix, train_labels, test_matrix, true_labels):
        if not is_tar_archive(archive_path):
            raise ValueError(f'Expected tar/tar.gz archive: {archive_path}')
    if not label_key.is_file():
        raise FileNotFoundError(f'Label key file does not exist: {label_key}')

    output_dir.mkdir(parents=True, exist_ok=True)

    stratify_training(
        train_matrix=train_matrix,
        train_labels=train_labels,
        output_dir=output_dir,
        name=name,
        drop_ungated_training=args.drop_ungated_training,
    )
    stratify_test(
        test_matrix=test_matrix,
        true_labels=true_labels,
        output_dir=output_dir,
        name=name,
        drop_ungated_test=args.drop_ungated_test,
    )
    copy_if_needed(label_key, output_dir / f'{name}.label_key.json.gz')


if __name__ == '__main__':
    main()
