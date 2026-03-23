import argparse
import gzip
import logging
import os
import re
import shutil
import sqlite3
import threading
import time
from typing import List, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from tqdm import tqdm


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

BASE_URL = "https://ftp.ncbi.nlm.nih.gov/genomes/all"
DEFAULT_RETRIES = 3
DEFAULT_RETRY_DELAY = 5.0
DEFAULT_DOWNLOAD_WORKERS = 4
FAILED_ACCESSIONS_FILENAME = "failed_accessions.txt"
DIRECTORY_CACHE_FILENAME = "fetchm_sequence_cache.sqlite3"
REQUIRED_COLUMNS = {
    "Assembly Accession",
    "Assembly Name",
    "Host",
    "Collection Date",
    "Geographic Location",
    "Continent",
    "Subcontinent",
}


thread_local = threading.local()


def get_requests_session() -> requests.Session:
    session = getattr(thread_local, "requests_session", None)
    if session is None:
        session = requests.Session()
        thread_local.requests_session = session
    return session


class AssemblyDirectoryCache:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.lock = threading.Lock()
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._initialize()

    def _initialize(self) -> None:
        with self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS assembly_directory_cache (
                    assembly_accession TEXT PRIMARY KEY,
                    assembly_name TEXT,
                    assembly_directory TEXT
                )
                """
            )

    def get(self, assembly_accession: str, assembly_name: str) -> Optional[str]:
        with self.lock:
            row = self.conn.execute(
                """
                SELECT assembly_directory
                FROM assembly_directory_cache
                WHERE assembly_accession = ? AND assembly_name = ?
                """,
                (assembly_accession, normalize_assembly_name(assembly_name)),
            ).fetchone()
        return None if row is None else row[0]

    def set(self, assembly_accession: str, assembly_name: str, assembly_directory: str) -> None:
        with self.lock:
            self.conn.execute(
                """
                INSERT INTO assembly_directory_cache (
                    assembly_accession, assembly_name, assembly_directory
                ) VALUES (?, ?, ?)
                ON CONFLICT(assembly_accession) DO UPDATE SET
                    assembly_name = excluded.assembly_name,
                    assembly_directory = excluded.assembly_directory
                """,
                (assembly_accession, normalize_assembly_name(assembly_name), assembly_directory),
            )
            self.conn.commit()

    def close(self) -> None:
        with self.lock:
            self.conn.close()


def normalize_assembly_name(name: str) -> str:
    if pd.isna(name):
        return "NA"

    cleaned_name = str(name).strip()
    if not cleaned_name:
        return "NA"

    return cleaned_name.replace(" ", "_")


def build_parent_url(accession: str) -> str:
    accession_prefix, accession_digits = accession.split("_", 1)
    accession_core = accession_digits.split(".", 1)[0]
    dir1 = accession_core[:3]
    dir2 = accession_core[3:6]
    dir3 = accession_core[6:9]
    dir4 = accession_core[9:]
    return f"{BASE_URL}/{accession_prefix}/{dir1}/{dir2}/{dir3}/{dir4}"


def list_remote_assembly_directories(parent_url: str) -> List[str]:
    response = get_requests_session().get(parent_url, timeout=60)
    response.raise_for_status()
    return re.findall(r'href="([^"]+/)"', response.text)


def resolve_assembly_directory(
    accession: str,
    name: str,
    *,
    directory_cache: Optional[AssemblyDirectoryCache] = None,
) -> str:
    if directory_cache is not None:
        cached_directory = directory_cache.get(accession, name)
        if cached_directory:
            return cached_directory

    parent_url = build_parent_url(accession)
    normalized_name = normalize_assembly_name(name)
    candidate_directories = [
        f"{accession}_{normalized_name}",
        f"{accession}_NA",
    ]

    seen = set()
    unique_candidates = []
    for candidate in candidate_directories:
        if candidate not in seen:
            unique_candidates.append(candidate)
            seen.add(candidate)

    for candidate in unique_candidates:
        candidate_url = f"{parent_url}/{candidate}"
        response = get_requests_session().get(candidate_url, timeout=30)
        if response.ok:
            if directory_cache is not None:
                directory_cache.set(accession, name, candidate)
            return candidate

    directories = list_remote_assembly_directories(parent_url)
    matching_directories = [
        directory.rstrip("/")
        for directory in directories
        if directory.rstrip("/").startswith(f"{accession}_")
    ]

    if not matching_directories:
        raise FileNotFoundError(f"No remote assembly directory found for {accession}")

    expected_directory = f"{accession}_{normalized_name}"
    if expected_directory in matching_directories:
        return expected_directory

    fallback_directory = f"{accession}_NA"
    if fallback_directory in matching_directories:
        if directory_cache is not None:
            directory_cache.set(accession, name, fallback_directory)
        return fallback_directory

    resolved_directory = matching_directories[0]
    if directory_cache is not None:
        directory_cache.set(accession, name, resolved_directory)
    return resolved_directory


def download_genome_fasta_ftp(
    assembly_accession: str,
    assembly_name: str,
    output_folder: str,
    *,
    directory_cache: Optional[AssemblyDirectoryCache] = None,
) -> None:
    os.makedirs(output_folder, exist_ok=True)

    assembly_directory = resolve_assembly_directory(
        assembly_accession,
        assembly_name,
        directory_cache=directory_cache,
    )
    gz_basename = f"{assembly_directory}_genomic.fna.gz"
    fna_basename = f"{assembly_directory}_genomic.fna"
    url = f"{build_parent_url(assembly_accession)}/{assembly_directory}/{gz_basename}"

    gz_filename = os.path.join(output_folder, gz_basename)
    fna_filename = os.path.join(output_folder, fna_basename)

    if os.path.exists(fna_filename):
        logging.info("Genome FASTA already exists for %s: %s", assembly_accession, fna_filename)
        return

    response = get_requests_session().get(url, stream=True, timeout=300)
    response.raise_for_status()

    with open(gz_filename, "wb") as gz_file:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                gz_file.write(chunk)

    with gzip.open(gz_filename, "rb") as gz_file, open(fna_filename, "wb") as fna_file:
        shutil.copyfileobj(gz_file, fna_file)

    os.remove(gz_filename)
    logging.info("Downloaded genome FASTA for %s to %s", assembly_accession, fna_filename)


def download_with_retries(
    assembly_accession: str,
    assembly_name: str,
    output_folder: str,
    retries: int,
    retry_delay: float,
    *,
    directory_cache: Optional[AssemblyDirectoryCache] = None,
) -> None:
    for attempt in range(1, retries + 1):
        try:
            download_genome_fasta_ftp(
                assembly_accession,
                assembly_name,
                output_folder,
                directory_cache=directory_cache,
            )
            return
        except Exception as exc:
            logging.warning(
                "Download attempt %s/%s failed for %s: %s",
                attempt,
                retries,
                assembly_accession,
                exc,
            )

            normalized_name = normalize_assembly_name(assembly_name)
            partial_patterns = (
                f"{assembly_accession}_{normalized_name}_genomic.fna.gz",
                f"{assembly_accession}_NA_genomic.fna.gz",
            )
            for filename in partial_patterns:
                partial_path = os.path.join(output_folder, filename)
                if os.path.exists(partial_path):
                    os.remove(partial_path)

            if attempt == retries:
                raise

            time.sleep(retry_delay * attempt)


def load_input(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    missing_columns = REQUIRED_COLUMNS - set(df.columns)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(f"Required columns missing from input CSV: {missing}")
    return df


def get_expected_accessions(df: pd.DataFrame) -> List[str]:
    return df["Assembly Accession"].dropna().astype(str).tolist()


def get_downloaded_accessions(output_folder: str) -> Set[str]:
    downloaded = set()
    if not os.path.isdir(output_folder):
        return downloaded

    pattern = re.compile(r"^(GC[AF]_\d+\.\d+)_.*_genomic\.fna$")
    for filename in os.listdir(output_folder):
        match = pattern.match(filename)
        if match:
            downloaded.add(match.group(1))
    return downloaded


def write_failed_accessions(output_folder: str, failed_accessions: List[str]) -> str:
    failed_path = os.path.join(output_folder, FAILED_ACCESSIONS_FILENAME)
    with open(failed_path, "w", encoding="ascii") as handle:
        for accession in failed_accessions:
            handle.write(f"{accession}\n")
    return failed_path


def report_download_status(df: pd.DataFrame, output_folder: str) -> List[str]:
    expected_accessions = get_expected_accessions(df)
    expected_unique = set(expected_accessions)
    downloaded_accessions = get_downloaded_accessions(output_folder)
    missing_accessions = sorted(expected_unique - downloaded_accessions)
    downloaded_count = len(expected_unique & downloaded_accessions)
    total_count = len(expected_unique)
    percentage = 0.0 if total_count == 0 else (downloaded_count / total_count) * 100

    logging.info(
        "Download audit: %s/%s unique assemblies present in %s (%.2f%% complete).",
        downloaded_count,
        total_count,
        output_folder,
        percentage,
    )
    if missing_accessions:
        logging.warning("Missing assemblies after audit: %s", len(missing_accessions))
    else:
        logging.info("All requested assemblies are present in %s.", output_folder)

    return missing_accessions


def filter_dataframe(df: pd.DataFrame, args: argparse.Namespace) -> pd.DataFrame:
    filtered_df = df.copy()

    if args.host:
        host_filter = "|".join(args.host)
        filtered_df = filtered_df[filtered_df["Host"].str.contains(host_filter, case=False, na=False)]

    if args.year:
        filtered_df["Year"] = filtered_df["Collection Date"].astype(str).str.extract(r"(\d{4})")
        filtered_df = filtered_df.dropna(subset=["Year"])
        filtered_df["Year"] = filtered_df["Year"].astype(int)

        conditions = []
        for year_arg in args.year:
            if "-" in year_arg:
                start, end = year_arg.split("-", 1)
                conditions.append((filtered_df["Year"] >= int(start)) & (filtered_df["Year"] <= int(end)))
            else:
                year = int(year_arg)
                conditions.append(filtered_df["Year"] == year)

        if conditions:
            combined_condition = conditions[0]
            for condition in conditions[1:]:
                combined_condition = combined_condition | condition
            filtered_df = filtered_df[combined_condition]

        filtered_df = filtered_df.drop(columns=["Year"])

    if args.country:
        country_filter = "|".join(args.country)
        filtered_df = filtered_df[
            filtered_df["Geographic Location"].str.contains(country_filter, case=False, na=False)
        ]

    if args.cont:
        cont_filter = "|".join(args.cont)
        filtered_df = filtered_df[filtered_df["Continent"].str.contains(cont_filter, case=False, na=False)]

    if args.subcont:
        subcont_filter = [value.strip().lower() for value in args.subcont]
        filtered_df = filtered_df[filtered_df["Subcontinent"].str.lower().isin(subcont_filter)]

    return filtered_df


def add_sequence_arguments(
    parser: argparse.ArgumentParser,
    *,
    include_paths: bool = True,
    input_required: bool = True,
    outdir_required: bool = True,
) -> argparse.ArgumentParser:
    if include_paths:
        parser.add_argument("--input", required=input_required, help="Path to ncbi_clean.csv")
        parser.add_argument("--outdir", required=outdir_required, help="Directory where FASTA files will be written")
    parser.add_argument("--host", nargs="+", help='Filter by host species, e.g. "Homo sapiens"')
    parser.add_argument("--year", nargs="+", help='Filter by year or year range, e.g. "2015" "2018-2025"')
    parser.add_argument("--country", nargs="+", help='Filter by country, e.g. "Bangladesh" "United States"')
    parser.add_argument("--cont", nargs="+", help='Filter by continent, e.g. "Asia" "Africa"')
    parser.add_argument("--subcont", nargs="+", help='Filter by subcontinent, e.g. "Southern Asia"')
    parser.add_argument(
        "--retries",
        type=int,
        default=DEFAULT_RETRIES,
        help=f"Retry attempts per genome download (default: {DEFAULT_RETRIES})",
    )
    parser.add_argument(
        "--retry-delay",
        type=float,
        default=DEFAULT_RETRY_DELAY,
        help=f"Base delay in seconds before retrying a failed download (default: {DEFAULT_RETRY_DELAY})",
    )
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Only audit the output directory against the input CSV without downloading.",
    )
    parser.add_argument(
        "--download-workers",
        type=int,
        default=DEFAULT_DOWNLOAD_WORKERS,
        help=f"Concurrent genome download workers (default: {DEFAULT_DOWNLOAD_WORKERS})",
    )
    return parser


def build_sequence_parser(*, add_help: bool = True) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download genome FASTA files from an ncbi_clean.csv generated by fetchm.",
        add_help=add_help,
    )
    add_sequence_arguments(parser)
    return parser


def parse_args() -> argparse.Namespace:
    return build_sequence_parser().parse_args()


def download_single_row(
    row: pd.Series,
    output_dir: str,
    retries: int,
    retry_delay: float,
    directory_cache: AssemblyDirectoryCache,
) -> Optional[str]:
    assembly_accession = row["Assembly Accession"]
    assembly_name = row["Assembly Name"]
    try:
        download_with_retries(
            assembly_accession,
            assembly_name,
            output_dir,
            retries=retries,
            retry_delay=retry_delay,
            directory_cache=directory_cache,
        )
        return None
    except Exception as exc:
        logging.error("Failed to download %s after retries: %s", assembly_accession, exc)
        return str(assembly_accession)


def run_sequence_downloads(
    args: argparse.Namespace,
    *,
    input_path: Optional[str] = None,
    output_folder: Optional[str] = None,
) -> List[str]:
    input_file = input_path or args.input
    output_dir = output_folder or args.outdir

    if not input_file:
        raise ValueError("An input CSV path is required for sequence downloads.")

    if not output_dir:
        raise ValueError("An output directory is required for sequence downloads.")

    df = load_input(input_file)
    df = filter_dataframe(df, args)

    if df.empty:
        raise ValueError("No records match the provided filters.")

    os.makedirs(output_dir, exist_ok=True)

    if args.check_only:
        missing_accessions = report_download_status(df, output_dir)
        failed_path = write_failed_accessions(output_dir, missing_accessions)
        if missing_accessions:
            logging.warning("Wrote missing accessions to %s", failed_path)
        else:
            logging.info("Wrote an empty failure list to %s", failed_path)
        return missing_accessions

    directory_cache = AssemblyDirectoryCache(os.path.join(output_dir, DIRECTORY_CACHE_FILENAME))
    failed_accessions = []
    try:
        with ThreadPoolExecutor(max_workers=max(1, args.download_workers)) as executor:
            futures = [
                executor.submit(
                    download_single_row,
                    row,
                    output_dir,
                    args.retries,
                    args.retry_delay,
                    directory_cache,
                )
                for _, row in df.iterrows()
            ]
            for future in tqdm(as_completed(futures), total=len(futures), desc="Downloading genome FASTA files"):
                failed_accession = future.result()
                if failed_accession:
                    failed_accessions.append(failed_accession)
    finally:
        directory_cache.close()

    unique_failed_accessions = sorted(set(failed_accessions))
    success_count = len(set(get_expected_accessions(df))) - len(unique_failed_accessions)
    logging.info("Sequence downloading completed. Downloaded %s sequences.", success_count)
    missing_accessions = report_download_status(df, output_dir)
    failure_report = sorted(set(unique_failed_accessions) | set(missing_accessions))
    failed_path = write_failed_accessions(output_dir, failure_report)
    if unique_failed_accessions:
        logging.warning(
            "Failed accessions (%s): %s",
            len(unique_failed_accessions),
            ", ".join(unique_failed_accessions),
        )
    if failure_report:
        logging.warning("Wrote failed or missing accessions to %s", failed_path)
    else:
        logging.info("No failed or missing accessions. Wrote an empty failure list to %s", failed_path)
    return failure_report


def main() -> None:
    args = parse_args()

    try:
        run_sequence_downloads(args)
    except Exception as exc:
        logging.error("Sequence download failed: %s", exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
