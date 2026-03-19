# stratify

Python scaffold stage between data import and preprocessing.

- `data_stratify.py` reads imported `*.data.tar.gz` plus `*.order.json.gz`
- current behavior is pass-through only
- output contract matches data import so downstream stages can swap inputs cleanly

Local quick run:

```bash
bash run_data_stratify.sh
```
