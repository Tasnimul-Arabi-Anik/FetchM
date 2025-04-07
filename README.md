# FetchM: Metadata Fetching and Analysis Tool

## Overview
FetchM is a Python-based tool for fetching and analyzing genomic metadata from NCBI BioSample records. When you download ncbi_dataset.tsv from the NCBI genome database, the metadata fields such as 'Collection Date', 'Host', 'Geographic Location', and 'Isolation Source' are missing. This tool helps fetch the associated metadata for each BioSample ID. FetchM requires an input file (ncbi_dataset.tsv) from the NCBI genome database, retrieves additional annotations from NCBI, filters the data based on quality thresholds, and generates visualizations to help interpret the results. You can also download the filtered sequences. 

## Features
- Fetch metadata from NCBI BioSample API.
- Filter genomes based on CheckM completeness and ANI check status.
- Generate metadata summaries and annotation statistics.
- Create various visualizations for geographic distribution, collection dates, gene counts, continent, and subcontinent.
- Download genome sequences (optional).
- Download sequences after filtering by host species, year, country, continent, and subcontinent.

## Installation
### Install in a New Conda Environment 
```bash
conda create -n fetchm python=3.9
conda activate fetchm
pip install fetchM
```

## Usage
Run FetchM with the following command:
```bash
fetchM --input input.tsv --outdir results/
```
For retrieving all available sequence data
```bash
fetchM --input input.tsv --outdir results/ --checkm 0 --ani all
```
For downloading sequences with sequence information
```bash
fetchM --input input.tsv --outdir results/ --seq
```
For downloading filtered sequences add the available Options below:

### Options:
- `--checkm CHECKM` (Minimum CheckM completeness threshold, default: 95)
- `--ani` (Filter genomes by ANI status. Choices: OK, Inconclusive, Failed, all. Default is OK)
- `--sleep` (Time to wait between requests, default: 0.5s)
- `--seq` (Enable sequence download mode)

Downloading sequences based on different criteria
- `--host HOST [HOST ...]` (Filter by host species, e.g., `"Homo sapiens" "Bos taurus"`)
- `--year YEAR [YEAR ...]` (Filter by year or year range, e.g., `"2015" "2018-2025"`)
- `--country COUNTRY [COUNTRY ...]` (Filter by country, e.g., `"Bangladesh" "United States"`)
- `--cont CONT [CONT ...]` (Filter by continent, e.g., `"Asia" "Africa"`)
- `--subcont SUBCONT [SUBCONT ...]` (Filter by subcontinent, e.g., `"Southern Asia" "Western Africa"`)

## Input
Download ncbi_dataset.tsv of your target organism(s) from the [NCBI genome database](https://www.ncbi.nlm.nih.gov/datasets/genome/).
-**ncbi_dataset.tsv**

# 📋 Required Columns for `ncbi_dataset.tsv` in FetchM

Before running **FetchM**, ensure that your `ncbi_dataset.tsv` file includes the following columns. These columns are necessary for metadata enrichment, quality filtering, and downstream analysis.

---

## 🧬 Required Columns

| Column Name                                | Description |
|--------------------------------------------|-------------|
| `Assembly Accession`                       | Unique identifier for the assembly |
| `Assembly Name`                            | Name of the genome assembly |
| `Organism Name`                            | Scientific name of the organism |
| `ANI Check status`                         | Status of Average Nucleotide Identity (ANI) check |
| `Annotation Name`                          | Annotation version or label used |
| `Assembly Stats Total Sequence Length`     | Total length (in base pairs) of all sequences in the assembly |
| `Assembly BioProject Accession`            | Accession ID for the related BioProject |
| `Assembly BioSample Accession`             | Accession ID for the related BioSample |
| `Annotation Count Gene Total`              | Total number of genes annotated |
| `Annotation Count Gene Protein-coding`     | Number of protein-coding genes |
| `Annotation Count Gene Pseudogene`         | Number of pseudogenes |
| `CheckM completeness`                      | Completeness score from CheckM (in %) |
| `CheckM contamination`                     | Contamination score from CheckM (in %) |

---

## ✅ Tips

- The file must be **tab-separated** (`.tsv` format).
- Don't change Column headers 
---

## Output
FetchM creates a subdirectory in `/results/` based on the organism name provided in the input file. Inside this subdirectory, the following folders are created:
- **Metadata summaries** in `metadata_output/`
  - `annotation_summary.csv`
  - `assembly_summary.csv`
  - `metadata_summary.csv`
  - `ncbi_clean.csv`
  - `ncbi_filtered.csv`
  - `ncbi_dataset_updated.tsv`
- **Figures** in `figures/`
  - `Annotation Count Gene Protein-coding_distribution.tiff`
  - `Annotation Count Gene Pseudogene_distribution.tiff`
  - `Annotation Count Gene Total_distribution.tiff`
  - `Assembly Stats Total Sequence Length_distribution.tiff`
  - `Collection Date_bar_plots.tiff`
  - `Continent_bar_plots.tiff`
  - `Geographic Location_bar_plots.tiff`
  - `Geographic Location_map.jpg`
  - `Host_bar_plots.tiff`
  - `scatter_plot_gene_protein_coding_vs_collection_date.tiff`
  - `scatter_plot_gene_total_vs_collection_date.tiff`
  - `scatter_plot_total_sequence_length_vs_collection_date.tiff`
  - `Subcontinent_bar_plots.tiff`
- **Sequences** in `sequences/` (if `--seq` is enabled, it will contain the downloaded genome sequences).


## Visualizations
### Annotation Distributions
![Annotation Count Gene Protein-coding](figures/Annotation%20Count%20Gene%20Protein-coding_distribution.png)
![Annotation Count Gene Pseudogene](figures/Annotation%20Count%20Gene%20Pseudogene_distribution.png)
![Annotation Count Gene Total](figures/Annotation%20Count%20Gene%20Total_distribution.png)

### Assembly Statistics
![Assembly Sequence Length](figures/Assembly%20Stats%20Total%20Sequence%20Length_distribution.png)

### Metadata Summaries
![Collection Date Distribution](figures/Collection%20Date_bar_plots.png)
![Geographic Location Map](figures/Geographic%20Location_map.jpg)
![Geographic Location Distribution](figures/Geographic%20Location_bar_plots.png)
![Host Distribution](figures/Host_bar_plots.png)
![Continent Distribution](figures/Continent_bar_plots.png)
![Subcontinent Distribution](figures/Subcontinent_bar_plots.png)

### Scatter Plots
![Gene Protein Coding vs Collection Date](figures/scatter_plot_gene_protein_coding_vs_collection_date.png)
![Gene Total vs Collection Date](figures/scatter_plot_gene_total_vs_collection_date.png)
![Sequence Length vs Collection Date](figures/scatter_plot_Sequence_Length_vs_collection_date.png)

## License
This project is licensed under the MIT License.

## Author
Developed by Tasnimul Arabi Anik.

## Contributions
Contributions and improvements are welcome! Feel free to submit a pull request or report issues.

