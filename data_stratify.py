#!/usr/bin/env python3

"""Pass imported data archives through a stratify stage scaffold."""

from __future__ import annotations

import argparse
import gzip
import io
import json
import tarfile
from pathlib import Path
from typing import Any


TAR_GZIP_COMPRESSLEVEL = 1


def is_tar_archive(path: Path) -> bool:
    if not path.is_file():
        return False
    try:
        return tarfile.is_tarfile(path)
    except (OSError, tarfile.TarError):
        return False


def load_order_payload(path: Path) -> dict[str, Any]:
    with gzip.open(path, 'rt', encoding='utf-8') as handle:
        payload = json.load(handle)

    if not isinstance(payload, dict):
        raise ValueError('Order payload must be a JSON object.')

    order = payload.get('order')
    if not isinstance(order, list) or not order:
        raise ValueError("Order payload must contain a non-empty 'order' list.")
    if not all(isinstance(item, int) and item > 0 for item in order):
        raise ValueError("Order payload 'order' entries must be positive integers.")

    metadata = payload.get('metadata', {})
    if metadata is not None and not isinstance(metadata, dict):
        raise ValueError("Order payload 'metadata' must be a JSON object when provided.")

    return payload


def transform_member_bytes(member_name: str, data: bytes, mode: str) -> bytes:
    if mode == 'passthrough':
        return data
    raise ValueError(f'Unsupported stratify mode: {mode}')


def write_stratified_tar(input_path: Path, output_path: Path, mode: str) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    wrote_file = False
    with tarfile.open(input_path, mode='r:*') as source, tarfile.open(
        output_path, mode='w:gz', compresslevel=TAR_GZIP_COMPRESSLEVEL
    ) as target:
        for member in source.getmembers():
            if member.isdir():
                directory = tarfile.TarInfo(member.name)
                directory.type = tarfile.DIRTYPE
                directory.mode = member.mode
                directory.mtime = int(member.mtime)
                target.addfile(directory)
                continue

            if not member.isfile():
                continue

            extracted = source.extractfile(member)
            if extracted is None:
                raise ValueError(f'Failed to extract archive member: {member.name}')

            transformed = transform_member_bytes(member.name, extracted.read(), mode)
            tar_info = tarfile.TarInfo(member.name)
            tar_info.size = len(transformed)
            tar_info.mode = member.mode
            tar_info.mtime = int(member.mtime)
            target.addfile(tar_info, io.BytesIO(transformed))
            wrote_file = True

    if not wrote_file:
        raise ValueError(f'No regular files found in archive: {input_path}')


def write_order_payload(payload: dict[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(output_path, 'wt', encoding='utf-8') as handle:
        json.dump(payload, handle, indent=2)


def parse_args() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--data.raw',
        dest='data.raw',
        required=True,
        help='Path to imported tar.gz archive containing CSV sample files.',
    )
    parser.add_argument(
        '--data.order',
        dest='data.order',
        required=True,
        help='Path to order metadata JSON.GZ produced by data import.',
    )
    parser.add_argument('--output_dir', required=True)
    parser.add_argument('--name', default='dataset')
    parser.add_argument(
        '--mode',
        choices=['passthrough'],
        default='passthrough',
        help='Current stratify mode. Only pass-through is implemented for now.',
    )
    return parser


def main() -> None:
    args = parse_args().parse_args()

    raw_path = Path(getattr(args, 'data.raw'))
    order_path = Path(getattr(args, 'data.order'))
    output_dir = Path(args.output_dir)
    name = args.name

    if not is_tar_archive(raw_path):
        raise ValueError('--data.raw must be a tar/tar.gz archive.')
    if not order_path.is_file():
        raise FileNotFoundError(f'Order metadata file does not exist: {order_path}')

    order_payload = load_order_payload(order_path)

    write_stratified_tar(raw_path, output_dir / f'{name}.data.tar.gz', args.mode)
    write_order_payload(order_payload, output_dir / f'{name}.order.json.gz')


if __name__ == '__main__':
    main()
