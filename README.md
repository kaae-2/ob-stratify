# stratify

Python scaffold stage between preprocessing and analysis.

- `data_stratify.py` reads preprocessed train/test archives plus label metadata and the order/metadata JSON
- current behavior is pass-through unless `--drop-ungated-training` and/or `--drop-ungated-test` are enabled
- when enabled, rows with label `0` are dropped from the selected split(s) and the filtered tarballs are rewritten
- the order/metadata JSON is passed through unchanged for now
- output contract matches preprocessing/order inputs so analysis and metrics can swap inputs cleanly

Local quick run:

```bash
bash run_data_stratify.sh
```

The local helper also creates compatibility symlinks for local model bash scripts,
mapping `data_import.*` filenames onto the stratify outputs.
