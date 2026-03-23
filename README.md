# fetchm: Metadata Fetching and Analysis Tool

## Overview
`fetchm` is a command-line tool for bacterial comparative genomics workflows. It starts from an `ncbi_dataset.tsv` downloaded from the NCBI Genome interface, retrieves linked BioSample metadata, standardizes key fields, summarizes the dataset, generates figures, and can optionally download the filtered genome FASTA files.

The tool is intended primarily for bacterial genomes. Metadata structures differ across organism groups, so non-bacterial datasets may not behave consistently.

## Features
- Fetch `Isolation Source`, `Collection Date`, `Geographic Location`, and `Host` from NCBI BioSample.
- Filter records by ANI status and optional CheckM completeness threshold.
- Standardize common missing-value strings and harmonize collection year and country names.
- Generate summary tables, harmonization reports, and publication-ready plots.
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

## Usage
`fetchm` has three main commands:

```bash
fetchm metadata --input ncbi_dataset.tsv --outdir results/
fetchm run --input ncbi_dataset.tsv --outdir results/
fetchm seq --input results/<organism>/metadata_output/ncbi_clean.csv --outdir results/<organism>/sequence
```

Common examples:

```bash
fetchm metadata --input ncbi_dataset.tsv --outdir results/ --ani all
fetchm run --input ncbi_dataset.tsv --outdir results/ --checkm 95
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
- `--checkm` is optional. If you do not provide it, no CheckM filtering is applied.

## Output
For each run, `fetchm` creates an organism-specific result directory containing:

- `metadata_output/ncbi_dataset_updated.tsv`
- `metadata_output/ncbi_clean.csv`
- `metadata_output/metadata_summary.csv`
- `metadata_output/assembly_summary.csv`
- `metadata_output/annotation_summary.csv`
- `metadata_output/metadata_harmonization_report.csv`
- `figures/*.tiff`
- `figures/Geographic Location_map.jpg`
- `sequence/*.fna` when sequence downloading is enabled
- `sequence/failed_accessions.txt` after sequence audit or download

The harmonization report gives a quick completeness summary for the standardized metadata fields.

## Notes
- `fetchm run` already includes sequence downloading.
- `fetchm metadata` and `fetchm run` support `--ani`, `--checkm`, and `--sleep`.
- `fetchm seq` supports `--host`, `--year`, `--country`, `--cont`, `--subcont`, `--retries`, `--retry-delay`, and `--check-only`.
- Scatter plots are skipped automatically when the filtered dataset does not contain enough valid points.
- Runtime depends strongly on dataset size, NCBI responsiveness, and network conditions.

## License
MIT License.

## Author
Tasnimul Arabi Anik
