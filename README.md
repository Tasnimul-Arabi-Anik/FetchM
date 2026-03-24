# fetchm: Metadata Fetching and Analysis Tool

## Overview
`fetchm` is a command-line tool for bacterial comparative genomics workflows. It starts from an `ncbi_dataset.tsv` downloaded from the NCBI Genome interface, retrieves linked BioSample metadata, standardizes key fields, summarizes the dataset, generates figures, and can optionally download the filtered genome FASTA files.

The tool is intended primarily for bacterial genomes. Metadata structures differ across organism groups, so non-bacterial datasets may not behave consistently.

## Features
- Fetch `Isolation Source`, `Collection Date`, `Geographic Location`, and `Host` from NCBI BioSample.
- Apply optional ANI-status and CheckM completeness filtering.
- Standardize common missing-value strings and harmonize collection year and country names.
- Generate summary tables, harmonization reports, comprehensive Markdown and DOCX reports, and publication-ready plots.
- Download genome FASTA files from NCBI FTP after filtering by host, year, country, continent, or subcontinent.
- Audit an existing sequence directory with `--check-only`.

## Installation
Create a fresh environment and install from PyPI:

```bash
conda create -n fetchm python=3.9
conda activate fetchm
pip install fetchm
```

`fetchm` uses Python dependencies only. No separate `wget` installation is required for the current release.

## NCBI API Key
For faster metadata retrieval, you can provide an NCBI API key.

How to create one:

1. Sign in to your My NCBI account.
2. Open Account Settings.
3. Find `API Key Management`.
4. Create an API key.

Official NCBI references:

- https://www.ncbi.nlm.nih.gov/books/NBK25497/
- https://www.ncbi.nlm.nih.gov/books/NBK53593/
- https://www.ncbi.nlm.nih.gov/datasets/docs/v2/api/api-keys/

How `fetchm` uses request pacing:

- without an API key: default request delay is `0.34` seconds
- with an API key: default request delay is `0.15` seconds
- without an API key: default worker count is `3`
- with an API key: default worker count is `6`
- when NCBI returns `429 Too Many Requests`, `fetchm` now increases the shared request interval automatically and gradually relaxes it again after stable success

`fetchm` also keeps a persistent SQLite metadata cache inside each organism output directory so reruns do not need to refetch previously retrieved BioSample records.
Confirmed non-transient outcomes such as successful fetches and source-missing records are cached, while transient fetch failures are retried on later runs.
Sequence downloads also keep a small SQLite cache of resolved assembly directory paths inside the sequence output directory so reruns can skip repeated FTP path discovery.

You can pass the key directly:

```bash
fetchm metadata --input ncbi_dataset.tsv --outdir results/ --api-key YOUR_NCBI_API_KEY
```

Or use an environment variable:

```bash
export NCBI_API_KEY=YOUR_NCBI_API_KEY
fetchm metadata --input ncbi_dataset.tsv --outdir results/
```

Optional contact email:

```bash
fetchm metadata --input ncbi_dataset.tsv --outdir results/ --api-key YOUR_NCBI_API_KEY --email you@example.com
```

Optional worker override:

```bash
fetchm metadata --input ncbi_dataset.tsv --outdir results/ --api-key YOUR_NCBI_API_KEY --workers 8
```

Resume a previous metadata run without refetching already-resolved BioSamples:

```bash
fetchm metadata --input ncbi_dataset.tsv --outdir results/ --resume-metadata
```

Optional sequence download worker override:

```bash
fetchm seq --input ncbi_clean.csv --outdir sequence_output --download-workers 4
```

## Usage
`fetchm` has three main commands:

```bash
fetchm metadata --input ncbi_dataset.tsv --outdir results/
fetchm run --input ncbi_dataset.tsv --outdir results/
fetchm seq --input results/<organism>/metadata_output/ncbi_clean.csv --outdir results/<organism>/sequence
```

Common examples:

```bash
fetchm metadata --input ncbi_dataset.tsv --outdir results/
fetchm run --input ncbi_dataset.tsv --outdir results/ --checkm 95
fetchm metadata --input ncbi_dataset.tsv --outdir results/ --ani OK
fetchm seq --input ncbi_clean.csv --outdir sequence_output --country Bangladesh
fetchm seq --input ncbi_clean.csv --outdir sequence_output --cont Asia
fetchm seq --input ncbi_clean.csv --outdir sequence_output --check-only
```

Sequence filters:

```bash
fetchm seq \
  --input results/<organism>/metadata_output/ncbi_clean.csv \
  --outdir results/<organism>/sequence \
  --host "Homo sapiens" \
  --year 2018-2024 \
  --country Bangladesh
```

Legacy compatibility commands are still available:

```bash
fetchM --input ncbi_dataset.tsv --outdir results/
fetchM --input ncbi_dataset.tsv --outdir results/ --seq
fetchM-seq --input ncbi_clean.csv --outdir sequence_output
```

## Demo Files
Two example inputs are already bundled in the repository:

- `test.tsv`: quick smoke-test dataset.
- `Vibrio_v1.tsv`: the larger dataset used in the manuscript workflow.
- `figures/fetchm_workflow.svg`: workflow flowchart for GitHub/documentation.
- `figures/fetchm_workflow.tiff`: 600 dpi manuscript-ready workflow figure.

Quick smoke test:

```bash
fetchm metadata --input test.tsv --outdir test_output
```

## Input Requirements
Download `ncbi_dataset.tsv` from the [NCBI Genome Datasets interface](https://www.ncbi.nlm.nih.gov/datasets/genome/).

If you are unsure which export options to pick, selecting all available columns in the NCBI table export is the safest route.

Required columns:

| Column Name | Description |
| --- | --- |
| `Assembly Accession` | Unique identifier for the assembly |
| `Assembly Name` | Name of the genome assembly |
| `Organism Name` | Scientific name of the organism |
| `ANI Check status` | ANI validation status from NCBI |
| `Annotation Name` | Annotation pipeline name |
| `Assembly Stats Total Sequence Length` | Total sequence length |
| `Assembly BioProject Accession` | Linked BioProject accession |
| `Assembly BioSample Accession` | Linked BioSample accession |
| `Annotation Count Gene Total` | Total annotated genes |
| `Annotation Count Gene Protein-coding` | Protein-coding genes |
| `Annotation Count Gene Pseudogene` | Pseudogenes |
| `CheckM completeness` | CheckM completeness value |
| `CheckM contamination` | CheckM contamination value |

Tips:

- The file must be tab-separated.
- Keep the original header names unchanged.
- ANI filtering is disabled by default. Use `--ani OK`, `--ani Failed`, or other explicit values only when you want filtering.
- `--checkm` is optional. If you do not provide it, no CheckM filtering is applied.

## Output
For each run, `fetchm` creates an organism-specific result directory containing:

- `metadata_output/ncbi_dataset_updated.tsv`
- `metadata_output/ncbi_clean.csv`
- `metadata_output/metadata_summary.csv`
- `metadata_output/assembly_summary.csv`
- `metadata_output/annotation_summary.csv`
- `metadata_output/metadata_harmonization_report.csv`
- `metadata_output/metadata_fetch_failures.csv`
- `metadata_output/fetchm_report.md`
- `metadata_output/fetchm_report.docx`
- `figures/*.tiff`
- `figures/Geographic Location_map.jpg`
- `sequence/*.fna` when sequence downloading is enabled
- `sequence/failed_accessions.txt` after sequence audit or download

The harmonization report gives a quick completeness summary for the standardized metadata fields.
The comprehensive reports summarize runtime, filters, metadata completeness, key observations, numeric summaries, generated outputs, and fetch-failure reasons.
Metadata fetching now uses the BioSample E-utilities XML route first and falls back to the NCBI BioSample summary payload when the primary record is incomplete or resolves to the wrong accession.

Missing metadata semantics:

- `unknown`: the source metadata explicitly used a missing or unknown-style value such as `NA`, `missing`, or `unknown`
- `absent`: FetchM could not retrieve or locate a usable value for that field from the linked metadata
- `Metadata Fetch Status` values include `ok`, `cached`, `source_missing`, `not_found`, and `fetch_failed`

Additional metadata notes:

- Geographic labels such as `Taiwan`, `Hong Kong`, `Guam`, and `Republic of the Congo` are normalized for continent and subcontinent assignment.
- Isolation-source classification is conservative: rows with source-like attributes but missing-style values are treated as `unknown`, while broad source absence remains `absent`.

## Notes
- `fetchm run` already includes sequence downloading.
- `fetchm metadata` and `fetchm run` support `--ani`, `--checkm`, `--sleep`, `--api-key`, `--email`, and `--workers`.
- `fetchm metadata` also supports `--resume-metadata` for rerunning only unresolved metadata rows from a previous output directory.
- `fetchm seq` supports `--host`, `--year`, `--country`, `--cont`, `--subcont`, `--retries`, `--retry-delay`, `--check-only`, and `--download-workers`.
- Scatter plots are skipped automatically when the filtered dataset does not contain enough valid points.
- Successful runs now report total runtime together with the number of NCBI input rows processed.
- Runtime depends strongly on dataset size, NCBI responsiveness, and network conditions.

## License
MIT License.

## Author
Tasnimul Arabi Anik
