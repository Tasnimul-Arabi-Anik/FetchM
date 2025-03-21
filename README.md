# FetchM: Metadata Fetching and Analysis Tool

## Overview
FetchM is a Python-based tool for fetching and analyzing genomic metadata from NCBI BioSample records. When you download ncbi_dataset.tsv from the NCBI genome database, the metadata fields such as 'Collection Date', 'Host', 'Geographic Location', and 'Isolation Source' are missing. This tool helps fetch the associated metadata for each BioSample ID. FetchM requires an input file (ncbi_dataset.tsv) from the NCBI genome database, retrieves additional annotations from NCBI, filters the data based on quality thresholds, and generates visualizations to help interpret the results. You can also download the filtered sequences. 

## Features
- Fetch metadata from NCBI BioSample API.
- Filter genomes based on CheckM completeness and ANI check status.
- Generate metadata summaries and annotation statistics.
- Create various visualizations for geographic distribution, collection dates, and gene counts.
- Download genome sequences (optional).

## Installation
### Using Conda
You can install FetchM in a Conda environment:
```bash
conda create -n fetchM_env python=3.8
conda activate fetchM_env
conda create -n fetchM_env -c conda-forge python=3.8 pandas requests xmltodict matplotlib seaborn scipy tqdm
```

### Using pip
Ensure you have Python 3 installed. Install dependencies with:
```bash
pip install -r requirements.txt
```

## Usage
Run FetchM with the following command:
```bash
fetchM --input input.tsv --outdir results/
```

### Additional Options:
- `--checkm 95` (Set CheckM completeness threshold, default: 95)
- `--seq` (Enable sequence download mode)

## Output
FetchM creates multiple output files inside the `results/` directory:
- **Metadata summaries** in `metadata_output/`
- **Figures** in `figures/`
- **Filtered datasets** for further analysis

## Visualizations
### Annotation Distributions
![Annotation Count Gene Protein-coding](figures/Annotation%20Count%20Gene%20Protein-coding_distribution.png)
![Annotation Count Gene Pseudogene](figures/Annotation%20Count%20Gene%20Pseudogene_distribution.png)
![Annotation Count Gene Total](figures/Annotation%20Count%20Gene%20Total_distribution.png)

### Assembly Statistics
![Assembly Sequence Length](figures/Assembly%20Stats%20Total%20Sequence%20Length_distribution.png)

### Metadata Summaries
![Collection Date Distribution](figures/Collection%20Date_bar_plots.png)
![Geographic Location Distribution](figures/Geographic%20Location_bar_plots.png)
![Host Distribution](figures/Host_bar_plots.png)

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

