# Third-party notices

## FinQA smoke fixture

The files under `tests/fixtures/finqa/` contain three small records selected from the `dev` split of [czyssrs/FinQA](https://github.com/czyssrs/FinQA), pinned to revision `0f16e2867befa6840783e58be38c9efb9229d742`.

The fixture is retained only for deterministic local smoke tests. Its source and byte hashes are recorded in [`MANIFEST.json`](tests/fixtures/finqa/MANIFEST.json). The pinned upstream repository snapshot did not provide a standard SPDX license declaration in the project metadata inspected for this fixture. Therefore this repository does **not** relicense the records as MIT, and downstream redistribution should independently confirm the upstream dataset and source-document terms before using the fixture outside testing.

Attribution: *FinQA: A Dataset of Numerical Reasoning over Financial Data*, Chen et al., EMNLP 2021. See the upstream repository and paper for the authors, citation, and current terms.

## This project

The scheduler source and documentation in this repository are released under the MIT License in [`LICENSE`](LICENSE). That license does not override third-party terms for the FinQA-derived fixture.
