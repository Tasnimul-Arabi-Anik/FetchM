import pandas as pd
import requests
import xmltodict
import time
import matplotlib.pyplot as plt
import os
import seaborn as sns
import scipy.stats as stats
import argparse
import re
import sqlite3
import threading
import http.client
from tqdm import tqdm
import logging
from typing import Tuple, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import plotly.express as px
import plotly.io as pio

from fetchm.sequence import add_sequence_arguments, run_sequence_downloads

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Constants
NCBI_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
METADATA_FOLDER_NAME = "metadata_output"
FIGURES_FOLDER_NAME = "figures"
SEQUENCE_FOLDER_NAME = "sequence"
NCBI_TIMEOUT = 60
DEFAULT_SLEEP_NO_API_KEY = 0.34
DEFAULT_SLEEP_WITH_API_KEY = 0.15
DEFAULT_WORKERS_NO_API_KEY = 3
DEFAULT_WORKERS_WITH_API_KEY = 6
CACHE_NEGATIVE_RESULTS = False
DEFAULT_FETCH_RETRIES = 3
DEFAULT_RETRY_BACKOFF = 1.5
CACHE_DB_FILENAME = "fetchm_metadata_cache.sqlite3"
MISSING_VALUE_TOKENS = {
    "",
    "unknown",
    "unk",
    "unk.",
    "missing",
    "na",
    "n/a",
    "n.a.",
    "none",
    "null",
    "absent",
    "provided",
    "not collected",
    "not applicable",
    "not available",
    "not known",
    "no data",
    "no date",
    "no date information",
    "no date data",
    "no date specified",
    "no location",
    "no location information",
    "no location data",
    "no location specified",
    "no host",
    "no host information",
    "no host data",
    "no host specified",
    "not specified",
    "not provided",
    "missing data",
    "missing value",
    "unavailable",
}
DATE_YEAR_PATTERN = re.compile(r"(19|20)\d{2}")

# Cache to store fetched metadata
metadata_cache: Dict[str, Tuple] = {}
thread_local = threading.local()


def get_ncbi_session() -> requests.Session:
    session = getattr(thread_local, "ncbi_session", None)
    if session is None:
        session = requests.Session()
        thread_local.ncbi_session = session
    return session


class RequestRateLimiter:
    def __init__(self, interval_seconds: float) -> None:
        self.interval_seconds = max(interval_seconds, 0.0)
        self.lock = threading.Lock()
        self.next_allowed_time = 0.0

    def wait(self) -> None:
        with self.lock:
            now = time.monotonic()
            wait_time = max(0.0, self.next_allowed_time - now)
            scheduled_time = max(now, self.next_allowed_time) + self.interval_seconds
            self.next_allowed_time = scheduled_time
        if wait_time > 0:
            time.sleep(wait_time)


class MetadataPersistentCache:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.lock = threading.Lock()
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._initialize()

    def _initialize(self) -> None:
        with self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS biosample_metadata (
                    biosample_id TEXT PRIMARY KEY,
                    isolation_source TEXT,
                    collection_date TEXT,
                    geo_location TEXT,
                    host TEXT
                )
                """
            )

    def get(self, biosample_id: str) -> Optional[Tuple]:
        with self.lock:
            row = self.conn.execute(
                """
                SELECT isolation_source, collection_date, geo_location, host
                FROM biosample_metadata
                WHERE biosample_id = ?
                """,
                (biosample_id,),
            ).fetchone()
        if row is None:
            return None
        return tuple(pd.NA if value is None else value for value in row)

    def set(self, biosample_id: str, value: Tuple) -> None:
        serializable = tuple(None if pd.isna(item) else str(item) for item in value)
        with self.lock:
            self.conn.execute(
                """
                INSERT INTO biosample_metadata (
                    biosample_id, isolation_source, collection_date, geo_location, host
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(biosample_id) DO UPDATE SET
                    isolation_source = excluded.isolation_source,
                    collection_date = excluded.collection_date,
                    geo_location = excluded.geo_location,
                    host = excluded.host
                """,
                (biosample_id, *serializable),
            )
            self.conn.commit()

    def close(self) -> None:
        with self.lock:
            self.conn.close()


def load_data(input_file: str) -> pd.DataFrame:
    """Load the TSV file into a DataFrame."""
    try:
        df = pd.read_csv(input_file, sep='\t')
        logging.info(f"Data loaded successfully from {input_file}")
        return df
    except Exception as e:
        logging.error(f"Error loading data from {input_file}: {e}")
        raise


def filter_data(df: pd.DataFrame, checkm_threshold: float, ani_status_list: list) -> pd.DataFrame:
    """Filter the DataFrame based on CheckM completeness and ANI Check status."""
    try:
        filtered_df = df

        # Apply CheckM filtering only if threshold is provided
        if checkm_threshold is not None:
            filtered_df = filtered_df[
                filtered_df["CheckM completeness"].notna() &
                (filtered_df["CheckM completeness"] > checkm_threshold)
            ]

        # Apply ANI filtering only if 'all' is not in the list
        if "all" not in ani_status_list:
            filtered_df = filtered_df[filtered_df["ANI Check status"].isin(ani_status_list)]

        logging.info(f"Data filtered with CheckM threshold {checkm_threshold} and ANI status {ani_status_list}")
        return filtered_df

    except Exception as e:
        logging.error(f"Error filtering data: {e}")
        raise



def create_output_directory(output_directory: str, organism_name: str) -> Tuple[str, str, str, str]:
    """Create the output directory and subdirectories."""
    try:
        organism_folder = os.path.join(output_directory, organism_name.replace(" ", "_"))
        metadata_folder = os.path.join(organism_folder, METADATA_FOLDER_NAME)
        figures_folder = os.path.join(organism_folder, FIGURES_FOLDER_NAME)
        sequence_folder = os.path.join(organism_folder, SEQUENCE_FOLDER_NAME)

        os.makedirs(metadata_folder, exist_ok=True)
        os.makedirs(figures_folder, exist_ok=True)
        os.makedirs(sequence_folder, exist_ok=True)

        logging.info(f"Output directories created: {organism_folder}")
        return organism_folder, metadata_folder, figures_folder, sequence_folder
    except Exception as e:
        logging.error(f"Error creating output directories: {e}")
        raise



def get_effective_sleep(sleep_time: Optional[float], api_key: Optional[str]) -> float:
    if sleep_time is not None:
        return sleep_time
    return DEFAULT_SLEEP_WITH_API_KEY if api_key else DEFAULT_SLEEP_NO_API_KEY


def get_effective_workers(workers: Optional[int], api_key: Optional[str]) -> int:
    if workers is not None:
        return max(1, workers)
    return DEFAULT_WORKERS_WITH_API_KEY if api_key else DEFAULT_WORKERS_NO_API_KEY


def fetch_metadata(
    biosample_id: str,
    *,
    api_key: Optional[str] = None,
    email: Optional[str] = None,
    persistent_cache: Optional[MetadataPersistentCache] = None,
    rate_limiter: Optional[RequestRateLimiter] = None,
) -> Tuple:
    """Fetch metadata from NCBI."""
    if biosample_id in metadata_cache:
        return metadata_cache[biosample_id]

    if persistent_cache is not None:
        cached_value = persistent_cache.get(biosample_id)
        if cached_value is not None:
            metadata_cache[biosample_id] = cached_value
            return cached_value

    params = {
        "db": "biosample",
        "id": biosample_id,
        "retmode": "xml",
        "tool": "fetchm",
    }
    if api_key:
        params["api_key"] = api_key
    if email:
        params["email"] = email

    for attempt in range(1, DEFAULT_FETCH_RETRIES + 1):
        try:
            if rate_limiter is not None:
                rate_limiter.wait()

            response = get_ncbi_session().get(NCBI_URL, params=params, timeout=NCBI_TIMEOUT)
            response.raise_for_status()
            data = xmltodict.parse(response.text)

            biosample_set = data.get("BioSampleSet")
            if not biosample_set or not isinstance(biosample_set, dict):
                logging.warning(f"No 'BioSampleSet' found for BioSample {biosample_id}")
                return pd.NA, pd.NA, pd.NA, pd.NA

            biosample = biosample_set.get("BioSample")
            if isinstance(biosample, list):
                biosample = biosample[0] if biosample else None
            if not biosample or not isinstance(biosample, dict):
                logging.warning(f"No 'BioSample' found for BioSample {biosample_id}")
                return pd.NA, pd.NA, pd.NA, pd.NA

            attributes_node = biosample.get("Attributes") or {}
            if not isinstance(attributes_node, dict):
                logging.warning(f"No 'Attributes' found for BioSample {biosample_id}")
                return pd.NA, pd.NA, pd.NA, pd.NA

            attributes = attributes_node.get("Attribute", [])
            if isinstance(attributes, dict):
                attributes = [attributes]
            if not attributes:
                logging.warning(f"No 'Attributes' found for BioSample {biosample_id}")
                return pd.NA, pd.NA, pd.NA, pd.NA

            isolation_source = collection_date = geo_location = host = pd.NA
            for attr in attributes:
                if isinstance(attr, dict):
                    if attr.get("@attribute_name") == "isolation_source":
                        isolation_source = attr.get("#text", pd.NA)
                    elif attr.get("@attribute_name") == "collection_date":
                        collection_date = attr.get("#text", pd.NA)
                    elif attr.get("@attribute_name") == "geo_loc_name":
                        geo_location = attr.get("#text", pd.NA)
                    elif attr.get("@attribute_name") == "host":
                        host = attr.get("#text", pd.NA)

            metadata_tuple = (isolation_source, collection_date, geo_location, host)
            metadata_cache[biosample_id] = metadata_tuple
            if persistent_cache is not None:
                persistent_cache.set(biosample_id, metadata_tuple)
            return metadata_tuple
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response is not None else None
            if status_code in {429, 500, 502, 503, 504} and attempt < DEFAULT_FETCH_RETRIES:
                backoff_seconds = DEFAULT_RETRY_BACKOFF * attempt
                logging.warning(
                    "Transient HTTP error fetching BioSample %s on attempt %s/%s: %s. Retrying in %.1fs.",
                    biosample_id,
                    attempt,
                    DEFAULT_FETCH_RETRIES,
                    e,
                    backoff_seconds,
                )
                time.sleep(backoff_seconds)
                continue
            logging.error(f"Network error fetching BioSample {biosample_id}: {e}")
            break
        except requests.exceptions.RequestException as e:
            if attempt < DEFAULT_FETCH_RETRIES:
                cause = getattr(e, "__cause__", None)
                if isinstance(cause, http.client.RemoteDisconnected) or "RemoteDisconnected" in str(e):
                    backoff_seconds = DEFAULT_RETRY_BACKOFF * attempt
                    logging.warning(
                        "Transient connection error fetching BioSample %s on attempt %s/%s: %s. Retrying in %.1fs.",
                        biosample_id,
                        attempt,
                        DEFAULT_FETCH_RETRIES,
                        e,
                        backoff_seconds,
                    )
                    time.sleep(backoff_seconds)
                    continue
            logging.error(f"Network error fetching BioSample {biosample_id}: {e}")
            break
        except Exception as e:
            logging.error(f"Unexpected error fetching BioSample {biosample_id}: {e}")
            break

    if CACHE_NEGATIVE_RESULTS:
        metadata_cache[biosample_id] = (pd.NA, pd.NA, pd.NA, pd.NA)
    return pd.NA, pd.NA, pd.NA, pd.NA


def fetch_all_metadata(
    biosample_ids: List[object],
    *,
    api_key: Optional[str],
    email: Optional[str],
    persistent_cache: MetadataPersistentCache,
    request_interval: float,
    workers: int,
) -> Dict[str, Tuple]:
    rate_limiter = RequestRateLimiter(request_interval)
    unique_ids = []
    seen = set()
    for biosample_id in biosample_ids:
        if pd.isna(biosample_id):
            continue
        biosample_str = str(biosample_id)
        if biosample_str not in seen:
            seen.add(biosample_str)
            unique_ids.append(biosample_str)

    results: Dict[str, Tuple] = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {
            executor.submit(
                fetch_metadata,
                biosample_id,
                api_key=api_key,
                email=email,
                persistent_cache=persistent_cache,
                rate_limiter=rate_limiter,
            ): biosample_id
            for biosample_id in unique_ids
        }
        for future in tqdm(as_completed(future_map), total=len(future_map), desc="Fetching metadata"):
            biosample_id = future_map[future]
            results[biosample_id] = future.result()

    return results


def standardize_date(date: str) -> str:
    """Standardize the 'Collection Date' column."""
    if pd.isna(date):
        return "absent"

    cleaned = str(date).strip()
    if not cleaned:
        return "absent"

    if cleaned.lower() in MISSING_VALUE_TOKENS:
        return "absent"

    match = DATE_YEAR_PATTERN.search(cleaned)
    if not match:
        return "absent"

    year = int(match.group(0))
    current_year = pd.Timestamp.utcnow().year
    if 1900 <= year <= current_year:
        return str(year)

    return "absent"


def normalize_missing_string(value: object) -> Optional[str]:
    if pd.isna(value):
        return None

    normalized = " ".join(str(value).strip().split())
    if not normalized or normalized.lower() in MISSING_VALUE_TOKENS:
        return None

    return normalized


def normalize_title_case(value: str) -> str:
    tokens = re.split(r"(\W+)", value)
    normalized_tokens = []
    for token in tokens:
        if token.isalpha() and token.upper() not in {"USA", "UK", "UAE", "DRC"}:
            normalized_tokens.append(token.capitalize())
        else:
            normalized_tokens.append(token)
    return "".join(normalized_tokens)


def standardize_text_field(value: str) -> str:
    normalized = normalize_missing_string(value)
    if normalized is None:
        return "absent"

    return normalized


def standardize_location(location: str) -> str:
    """Standardize the 'Geographic Location' column."""
    normalized = normalize_missing_string(location)
    if normalized is None:
        return "absent"

    country = normalized.split(":")[0].strip()
    normalized_country = normalize_country_name(country)
    return normalized_country if normalized_country else "absent"


def standardize_host(host: str) -> str:
    """Standardize the 'Host' column."""
    return standardize_text_field(host)


def standardize_isolation_source(source: str) -> str:
    """Standardize the 'Isolation Source' column."""
    return standardize_text_field(source)


def save_summary(df: pd.DataFrame, output_file: str) -> None:
    """Save the DataFrame to a TSV file."""
    try:
        df.to_csv(output_file, sep='\t', index=False)
        logging.info(f"Data saved to {output_file}")
    except Exception as e:
        logging.error(f"Error saving data to {output_file}: {e}")
        raise


def plot_bar_charts(variable: str, frequency: pd.Series, percentage: pd.Series, figures_folder: str) -> None:
    """Generate and save bar plots for a given variable with dynamic width."""
    try:
        num_categories = len(frequency)
        width_per_category = 0.4  # adjust this factor as needed
        min_width = 10
        max_width = 30
        fig_width = min(max(num_categories * width_per_category, min_width), max_width)
        frequency_df = pd.DataFrame({variable: frequency.index, "Value": frequency.values})
        percentage_df = pd.DataFrame({variable: percentage.index, "Value": percentage.values})

        plt.figure(figsize=(fig_width, 6))

        # Frequency plot
        plt.subplot(1, 2, 1)
        sns.barplot(data=frequency_df, x=variable, y="Value", hue=variable, palette="viridis", dodge=False, legend=False)
        plt.title(f"Frequency of {variable}")
        plt.xlabel(variable)
        plt.ylabel("Frequency")
        plt.xticks(rotation=45, ha="right")

        # Percentage plot
        plt.subplot(1, 2, 2)
        sns.barplot(data=percentage_df, x=variable, y="Value", hue=variable, palette="viridis", dodge=False, legend=False)
        plt.title(f"Percentage of {variable}")
        plt.xlabel(variable)
        plt.ylabel("Percentage")
        plt.xticks(rotation=45, ha="right")

        plt.tight_layout()
        figure_path = os.path.join(figures_folder, f"{variable}_bar_plots.tiff")
        plt.savefig(figure_path, format="tiff", dpi=300)
        plt.close()
        logging.info(f"Bar plots saved for {variable}")
    except Exception as e:
        logging.error(f"Error generating bar plots for {variable}: {e}")
        raise


def plot_geo_choropleth(variable: str, frequency: pd.Series, figures_folder: str) -> None:
    """Generate and save a choropleth map for a given geographic variable."""
    try:
        # Create DataFrame
        map_df = frequency.reset_index()
        map_df.columns = [variable, 'Frequency']

        # Create hover text
        map_df['hover'] = map_df.apply(
            lambda row: f"{row[variable]}<br>Count: {row['Frequency']}", axis=1
        )

        # Generate choropleth
        fig = px.choropleth(
            map_df,
            locations=variable,
            locationmode="country names",
            color="Frequency",
            hover_name=variable,
            hover_data={"Frequency": True},
            color_continuous_scale="Viridis",
            title=f"Geographic Distribution",
            template="plotly_white"
        )

        # Update layout for improved appearance
        fig.update_layout(
            geo=dict(
                projection_type="natural earth",  # better projection
                showframe=False,
                showcoastlines=True,
                coastlinecolor="gray",
                landcolor="white",
                showcountries=True,
                countrycolor="black"
            ),
            coloraxis_colorbar=dict(
                title="Record Count",
                ticks="outside"
            ),
            margin=dict(l=20, r=20, t=60, b=20)
        )

        # Save as high-resolution image
        figure_path = os.path.join(figures_folder, f"{variable}_map.jpg")
        pio.write_image(fig, figure_path, format="jpg", scale=4)

        logging.info(f"Map plot saved for {variable}")

    except Exception as e:
        logging.error(f"Error generating map plot for {variable}: {e}")
        raise


def plot_distribution(column: str, data: pd.Series, title: str, figures_folder: str) -> None:
    """Generate and save a distribution plot."""
    try:
        plt.figure(figsize=(8, 6))
        sns.histplot(data, kde=True, color="blue")
        plt.title(f"Distribution of {title}")
        plt.xlabel(title)
        plt.ylabel("Frequency")
        plt.tight_layout()
        figure_path = os.path.join(figures_folder, f"{column}_distribution.tiff")
        plt.savefig(figure_path, format="tiff", dpi=300)
        plt.close()
        logging.info(f"Distribution plot saved for {column}")
    except Exception as e:
        logging.error(f"Error generating distribution plot for {column}: {e}")
        raise


def plot_scatter_with_trend_and_corr(
    x: pd.Series, y: pd.Series, xlabel: str, ylabel: str, title: str, filename: str, figures_folder: str
) -> None:
    """Generate and save a scatter plot with a trend line and correlation coefficient."""
    try:
        plt.figure(figsize=(10, 6))

        x_numeric = pd.to_numeric(x, errors="coerce").round().astype('Int64')
        y_numeric = pd.to_numeric(y, errors="coerce")

        plot_data = pd.DataFrame({'x': x_numeric, 'y': y_numeric}).dropna()

        if len(plot_data) < 2:
            logging.warning("Skipping %s because fewer than two valid data points are available.", title)
            return

        # Outlier removal (z-score >3)
        # IQR-based outlier removal based only on x-axis
        Q1 = plot_data['x'].quantile(0.25)
        Q3 = plot_data['x'].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR

        filtered_data = plot_data[(plot_data['x'] >= lower_bound) & (plot_data['x'] <= upper_bound)]

        if len(filtered_data) < 2:
            logging.warning("Skipping %s because fewer than two points remain after filtering.", title)
            return

        if filtered_data["x"].nunique() < 2 or filtered_data["y"].nunique() < 2:
            logging.warning("Skipping %s because the remaining data have no variance.", title)
            return

        # Correlations
        pearson_r, pearson_p = stats.pearsonr(filtered_data['x'], filtered_data['y'])
        spearman_r, spearman_p = stats.spearmanr(filtered_data['x'], filtered_data['y'])

        # Plot
        sns.regplot(
            x=filtered_data['x'],
            y=filtered_data['y'],
            scatter_kws={"alpha": 0.5, "s": 40},
            line_kws={"color": "red"}
        )
        plt.gca().xaxis.set_major_locator(plt.MaxNLocator(integer=True))
        plt.xticks(rotation=90)
        plt.grid(True, linestyle='--', alpha=0.5)

        p_text = lambda p: "< 0.001" if p < 0.001 else f"{p:.3f}"

        # Annotation
        annotation_text = (
            f"Pearson r = {pearson_r:.3f} (p={p_text(pearson_p)})\n"
            f"Spearman ρ = {spearman_r:.3f} (p={p_text(spearman_p)})"
            )
        y_pos = filtered_data['y'].max() - 0.1 * (filtered_data['y'].max() - filtered_data['y'].min())

        plt.text(
            filtered_data['x'].min(),
            y_pos,
            annotation_text,
            fontsize=11,
            color="black",
            bbox=dict(facecolor="white", alpha=0.7)
        )

        plt.title(title)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.tight_layout()

        figure_path = os.path.join(figures_folder, filename)
        plt.savefig(figure_path, format="tiff", dpi=300)
        plt.close()
        logging.info(f"Scatter plot saved: {title}")

    except Exception as e:
        logging.error(f"Error generating scatter plot for {title}: {e}")
        raise


def save_clean_data(df: pd.DataFrame, columns_to_keep: List[str], output_file: str) -> None:
    """Save a filtered DataFrame with selected columns."""
    try:
        df_filtered = df[columns_to_keep]
        df_filtered.to_csv(output_file, index=False)
        logging.info(f"Filtered dataset saved to {output_file}")
    except Exception as e:
        logging.error(f"Error saving filtered dataset to {output_file}: {e}")
        raise


def generate_metadata_summary(df: pd.DataFrame, output_file: str) -> None:
    """Generate and save a metadata summary."""
    try:
        summary_data = []
        for column in ["Geographic Location", "Host", "Collection Date"]:
            value_counts = df[column].value_counts()
            total = value_counts.sum()
            for value, count in value_counts.items():
                percentage = (count / total) * 100
                summary_data.append([column, value, count, f"{percentage:.2f}%"])

        summary_df = pd.DataFrame(summary_data, columns=["Variable", "Value", "Frequency", "Percentage"])
        summary_df.to_csv(output_file, index=False)
        logging.info(f"Metadata summary saved to {output_file}")
    except Exception as e:
        logging.error(f"Error generating metadata summary: {e}")
        raise


def generate_harmonization_report(df: pd.DataFrame, output_file: str) -> None:
    """Generate a completeness report for harmonized metadata fields."""
    try:
        tracked_columns = [
            "Isolation Source",
            "Collection Date",
            "Geographic Location",
            "Host",
            "Continent",
            "Subcontinent",
        ]
        total_rows = len(df)
        rows = []
        for column in tracked_columns:
            absent_count = int((df[column] == "absent").sum()) if column in df.columns else 0
            unknown_count = int((df[column] == "Unknown").sum()) if column in df.columns else 0
            present_count = total_rows - absent_count - unknown_count
            completeness = 0.0 if total_rows == 0 else (present_count / total_rows) * 100
            rows.append(
                {
                    "Variable": column,
                    "Total Records": total_rows,
                    "Present Records": present_count,
                    "Absent Records": absent_count,
                    "Unknown Records": unknown_count,
                    "Completeness (%)": f"{completeness:.2f}",
                }
            )

        pd.DataFrame(rows).to_csv(output_file, index=False)
        logging.info("Metadata harmonization report saved to %s", output_file)
    except Exception as e:
        logging.error(f"Error generating metadata harmonization report: {e}")
        raise


def generate_annotation_summary(df: pd.DataFrame, output_file: str) -> None:
    """Generate and save an annotation summary."""
    try:
        df = df.copy()
        summary_data = []
        for column in ["Annotation Count Gene Total", "Annotation Count Gene Protein-coding", "Annotation Count Gene Pseudogene"]:
            numeric_values = pd.to_numeric(df[column], errors="coerce").dropna()
            if not numeric_values.empty:
                highest = numeric_values.max()
                mean = numeric_values.mean()
                median = numeric_values.median()
                lowest = numeric_values.min()
            else:
                highest = mean = median = lowest = "No Data"

            summary_data.append([column, "Highest", highest])
            summary_data.append([column, "Mean", mean])
            summary_data.append([column, "Median", median])
            summary_data.append([column, "Lowest", lowest])

        summary_df = pd.DataFrame(summary_data, columns=["Variable", "Summary", "Value"])
        summary_df.to_csv(output_file, index=False)
        logging.info(f"Annotation summary saved to {output_file}")
    except Exception as e:
        logging.error(f"Error generating annotation summary: {e}")
        raise


def generate_assembly_summary(df: pd.DataFrame, output_file: str) -> None:
    """Generate and save an assembly summary."""
    try:
        numeric_values = pd.to_numeric(df["Assembly Stats Total Sequence Length"], errors="coerce").dropna()
        if not numeric_values.empty:
            highest = numeric_values.max()
            mean = numeric_values.mean()
            median = numeric_values.median()
            lowest = numeric_values.min()
        else:
            highest = mean = median = lowest = "No Data"

        summary_data = [
            ["Assembly Stats Total Sequence Length", "Highest", highest],
            ["Assembly Stats Total Sequence Length", "Mean", mean],
            ["Assembly Stats Total Sequence Length", "Median", median],
            ["Assembly Stats Total Sequence Length", "Lowest", lowest]
        ]

        summary_df = pd.DataFrame(summary_data, columns=["Variable", "Summary", "Value"])
        summary_df.to_csv(output_file, index=False)
        logging.info(f"Assembly summary saved to {output_file}")
    except Exception as e:
        logging.error(f"Error generating assembly summary: {e}")
        raise


COUNTRY_MAPPING = {
    # Africa (54 countries)
    "Algeria": {"Continent": "Africa", "Subcontinent": "Northern Africa"},
    "Angola": {"Continent": "Africa", "Subcontinent": "Middle Africa"},
    "Benin": {"Continent": "Africa", "Subcontinent": "Western Africa"},
    "Botswana": {"Continent": "Africa", "Subcontinent": "Southern Africa"},
    "Burkina Faso": {"Continent": "Africa", "Subcontinent": "Western Africa"},
    "Burundi": {"Continent": "Africa", "Subcontinent": "Eastern Africa"},
    "Cabo Verde": {"Continent": "Africa", "Subcontinent": "Western Africa"},
    "Cameroon": {"Continent": "Africa", "Subcontinent": "Middle Africa"},
    "Central African Republic": {"Continent": "Africa", "Subcontinent": "Middle Africa"},
    "Chad": {"Continent": "Africa", "Subcontinent": "Middle Africa"},
    "Comoros": {"Continent": "Africa", "Subcontinent": "Eastern Africa"},
    "Congo": {"Continent": "Africa", "Subcontinent": "Middle Africa"},
    "Democratic Republic of the Congo": {"Continent": "Africa", "Subcontinent": "Middle Africa"},
    "Djibouti": {"Continent": "Africa", "Subcontinent": "Eastern Africa"},
    "Egypt": {"Continent": "Africa", "Subcontinent": "Northern Africa"},
    "Equatorial Guinea": {"Continent": "Africa", "Subcontinent": "Middle Africa"},
    "Eritrea": {"Continent": "Africa", "Subcontinent": "Eastern Africa"},
    "Eswatini": {"Continent": "Africa", "Subcontinent": "Southern Africa"},
    "Ethiopia": {"Continent": "Africa", "Subcontinent": "Eastern Africa"},
    "Gabon": {"Continent": "Africa", "Subcontinent": "Middle Africa"},
    "Gambia": {"Continent": "Africa", "Subcontinent": "Western Africa"},
    "Ghana": {"Continent": "Africa", "Subcontinent": "Western Africa"},
    "Guinea": {"Continent": "Africa", "Subcontinent": "Western Africa"},
    "Guinea-Bissau": {"Continent": "Africa", "Subcontinent": "Western Africa"},
    "Ivory Coast": {"Continent": "Africa", "Subcontinent": "Western Africa"},
    "Kenya": {"Continent": "Africa", "Subcontinent": "Eastern Africa"},
    "Lesotho": {"Continent": "Africa", "Subcontinent": "Southern Africa"},
    "Liberia": {"Continent": "Africa", "Subcontinent": "Western Africa"},
    "Libya": {"Continent": "Africa", "Subcontinent": "Northern Africa"},
    "Madagascar": {"Continent": "Africa", "Subcontinent": "Eastern Africa"},
    "Malawi": {"Continent": "Africa", "Subcontinent": "Eastern Africa"},
    "Mali": {"Continent": "Africa", "Subcontinent": "Western Africa"},
    "Mauritania": {"Continent": "Africa", "Subcontinent": "Western Africa"},
    "Mauritius": {"Continent": "Africa", "Subcontinent": "Eastern Africa"},
    "Mayotte": {"Continent": "Africa", "Subcontinent": "Eastern Africa"},
    "Morocco": {"Continent": "Africa", "Subcontinent": "Northern Africa"},
    "Mozambique": {"Continent": "Africa", "Subcontinent": "Eastern Africa"},
    "Namibia": {"Continent": "Africa", "Subcontinent": "Southern Africa"},
    "Niger": {"Continent": "Africa", "Subcontinent": "Western Africa"},
    "Nigeria": {"Continent": "Africa", "Subcontinent": "Western Africa"},
    "Rwanda": {"Continent": "Africa", "Subcontinent": "Eastern Africa"},
    "Sao Tome and Principe": {"Continent": "Africa", "Subcontinent": "Middle Africa"},
    "Senegal": {"Continent": "Africa", "Subcontinent": "Western Africa"},
    "Seychelles": {"Continent": "Africa", "Subcontinent": "Eastern Africa"},
    "Sierra Leone": {"Continent": "Africa", "Subcontinent": "Western Africa"},
    "Somalia": {"Continent": "Africa", "Subcontinent": "Eastern Africa"},
    "South Africa": {"Continent": "Africa", "Subcontinent": "Southern Africa"},
    "South Sudan": {"Continent": "Africa", "Subcontinent": "Eastern Africa"},
    "Sudan": {"Continent": "Africa", "Subcontinent": "Northern Africa"},
    "Tanzania": {"Continent": "Africa", "Subcontinent": "Eastern Africa"},
    "Togo": {"Continent": "Africa", "Subcontinent": "Western Africa"},
    "Tunisia": {"Continent": "Africa", "Subcontinent": "Northern Africa"},
    "Uganda": {"Continent": "Africa", "Subcontinent": "Eastern Africa"},
    "Zambia": {"Continent": "Africa", "Subcontinent": "Eastern Africa"},
    "Zimbabwe": {"Continent": "Africa", "Subcontinent": "Eastern Africa"},

    # Asia (48 countries)
    "Afghanistan": {"Continent": "Asia", "Subcontinent": "Southern Asia"},
    "Armenia": {"Continent": "Asia", "Subcontinent": "Western Asia"},
    "Azerbaijan": {"Continent": "Asia", "Subcontinent": "Western Asia"},
    "Bahrain": {"Continent": "Asia", "Subcontinent": "Western Asia"},
    "Bangladesh": {"Continent": "Asia", "Subcontinent": "Southern Asia"},
    "Bhutan": {"Continent": "Asia", "Subcontinent": "Southern Asia"},
    "Brunei": {"Continent": "Asia", "Subcontinent": "South-Eastern Asia"},
    "Cambodia": {"Continent": "Asia", "Subcontinent": "South-Eastern Asia"},
    "China": {"Continent": "Asia", "Subcontinent": "Eastern Asia"},
    "Cyprus": {"Continent": "Asia", "Subcontinent": "Western Asia"},
    "Georgia": {"Continent": "Asia", "Subcontinent": "Western Asia"},
    "India": {"Continent": "Asia", "Subcontinent": "Southern Asia"},
    "Indonesia": {"Continent": "Asia", "Subcontinent": "South-Eastern Asia"},
    "Iran": {"Continent": "Asia", "Subcontinent": "Southern Asia"},
    "Iraq": {"Continent": "Asia", "Subcontinent": "Western Asia"},
    "Israel": {"Continent": "Asia", "Subcontinent": "Western Asia"},
    "Japan": {"Continent": "Asia", "Subcontinent": "Eastern Asia"},
    "Jordan": {"Continent": "Asia", "Subcontinent": "Western Asia"},
    "Kazakhstan": {"Continent": "Asia", "Subcontinent": "Central Asia"},
    "Kuwait": {"Continent": "Asia", "Subcontinent": "Western Asia"},
    "Kyrgyzstan": {"Continent": "Asia", "Subcontinent": "Central Asia"},
    "Laos": {"Continent": "Asia", "Subcontinent": "South-Eastern Asia"},
    "Lebanon": {"Continent": "Asia", "Subcontinent": "Western Asia"},
    "Malaysia": {"Continent": "Asia", "Subcontinent": "South-Eastern Asia"},
    "Maldives": {"Continent": "Asia", "Subcontinent": "Southern Asia"},
    "Mongolia": {"Continent": "Asia", "Subcontinent": "Eastern Asia"},
    "Myanmar": {"Continent": "Asia", "Subcontinent": "South-Eastern Asia"},
    "Nepal": {"Continent": "Asia", "Subcontinent": "Southern Asia"},
    "North Korea": {"Continent": "Asia", "Subcontinent": "Eastern Asia"},
    "Oman": {"Continent": "Asia", "Subcontinent": "Western Asia"},
    "Pakistan": {"Continent": "Asia", "Subcontinent": "Southern Asia"},
    "Palestine": {"Continent": "Asia", "Subcontinent": "Western Asia"},
    "Philippines": {"Continent": "Asia", "Subcontinent": "South-Eastern Asia"},
    "Qatar": {"Continent": "Asia", "Subcontinent": "Western Asia"},
    "Russia": {"Continent": "Asia", "Subcontinent": "Northern Asia"},
    "Saudi Arabia": {"Continent": "Asia", "Subcontinent": "Western Asia"},
    "Singapore": {"Continent": "Asia", "Subcontinent": "South-Eastern Asia"},
    "South Korea": {"Continent": "Asia", "Subcontinent": "Eastern Asia"},
    "Sri Lanka": {"Continent": "Asia", "Subcontinent": "Southern Asia"},
    "Syria": {"Continent": "Asia", "Subcontinent": "Western Asia"},
    "Tajikistan": {"Continent": "Asia", "Subcontinent": "Central Asia"},
    "Thailand": {"Continent": "Asia", "Subcontinent": "South-Eastern Asia"},
    "Timor-Leste": {"Continent": "Asia", "Subcontinent": "South-Eastern Asia"},
    "Turkey": {"Continent": "Asia", "Subcontinent": "Western Asia"},
    "Turkmenistan": {"Continent": "Asia", "Subcontinent": "Central Asia"},
    "United Arab Emirates": {"Continent": "Asia", "Subcontinent": "Western Asia"},
    "Uzbekistan": {"Continent": "Asia", "Subcontinent": "Central Asia"},
    "Vietnam": {"Continent": "Asia", "Subcontinent": "South-Eastern Asia"},
    "Yemen": {"Continent": "Asia", "Subcontinent": "Western Asia"},

    # Europe (44 countries)
    "Albania": {"Continent": "Europe", "Subcontinent": "Southern Europe"},
    "Andorra": {"Continent": "Europe", "Subcontinent": "Southern Europe"},
    "Austria": {"Continent": "Europe", "Subcontinent": "Western Europe"},
    "Belarus": {"Continent": "Europe", "Subcontinent": "Eastern Europe"},
    "Belgium": {"Continent": "Europe", "Subcontinent": "Western Europe"},
    "Bosnia and Herzegovina": {"Continent": "Europe", "Subcontinent": "Southern Europe"},
    "Bulgaria": {"Continent": "Europe", "Subcontinent": "Eastern Europe"},
    "Croatia": {"Continent": "Europe", "Subcontinent": "Southern Europe"},
    "Czech Republic": {"Continent": "Europe", "Subcontinent": "Eastern Europe"},
    "Czechoslovakia": {"Continent": "Europe", "Subcontinent": "Eastern Europe"},
    "Denmark": {"Continent": "Europe", "Subcontinent": "Northern Europe"},
    "Estonia": {"Continent": "Europe", "Subcontinent": "Northern Europe"},
    "Finland": {"Continent": "Europe", "Subcontinent": "Northern Europe"},
    "France": {"Continent": "Europe", "Subcontinent": "Western Europe"},
    "Germany": {"Continent": "Europe", "Subcontinent": "Western Europe"},
    "Greece": {"Continent": "Europe", "Subcontinent": "Southern Europe"},
    "Hungary": {"Continent": "Europe", "Subcontinent": "Eastern Europe"},
    "Iceland": {"Continent": "Europe", "Subcontinent": "Northern Europe"},
    "Ireland": {"Continent": "Europe", "Subcontinent": "Northern Europe"},
    "Italy": {"Continent": "Europe", "Subcontinent": "Southern Europe"},
    "Latvia": {"Continent": "Europe", "Subcontinent": "Northern Europe"},
    "Liechtenstein": {"Continent": "Europe", "Subcontinent": "Western Europe"},
    "Lithuania": {"Continent": "Europe", "Subcontinent": "Northern Europe"},
    "Luxembourg": {"Continent": "Europe", "Subcontinent": "Western Europe"},
    "Malta": {"Continent": "Europe", "Subcontinent": "Southern Europe"},
    "Moldova": {"Continent": "Europe", "Subcontinent": "Eastern Europe"},
    "Monaco": {"Continent": "Europe", "Subcontinent": "Western Europe"},
    "Montenegro": {"Continent": "Europe", "Subcontinent": "Southern Europe"},
    "Netherlands": {"Continent": "Europe", "Subcontinent": "Western Europe"},
    "North Macedonia": {"Continent": "Europe", "Subcontinent": "Southern Europe"},
    "Norway": {"Continent": "Europe", "Subcontinent": "Northern Europe"},
    "Poland": {"Continent": "Europe", "Subcontinent": "Eastern Europe"},
    "Portugal": {"Continent": "Europe", "Subcontinent": "Southern Europe"},
    "Romania": {"Continent": "Europe", "Subcontinent": "Eastern Europe"},
    "San Marino": {"Continent": "Europe", "Subcontinent": "Southern Europe"},
    "Serbia": {"Continent": "Europe", "Subcontinent": "Southern Europe"},
    "Slovakia": {"Continent": "Europe", "Subcontinent": "Eastern Europe"},
    "Slovenia": {"Continent": "Europe", "Subcontinent": "Southern Europe"},
    "Spain": {"Continent": "Europe", "Subcontinent": "Southern Europe"},
    "Sweden": {"Continent": "Europe", "Subcontinent": "Northern Europe"},
    "Switzerland": {"Continent": "Europe", "Subcontinent": "Western Europe"},
    "Ukraine": {"Continent": "Europe", "Subcontinent": "Eastern Europe"},
    "United Kingdom": {"Continent": "Europe", "Subcontinent": "Northern Europe"},
    "Vatican City": {"Continent": "Europe", "Subcontinent": "Southern Europe"},

    # North America (23 countries)
    "Antigua and Barbuda": {"Continent": "North America", "Subcontinent": "Caribbean"},
    "Bahamas": {"Continent": "North America", "Subcontinent": "Caribbean"},
    "Barbados": {"Continent": "North America", "Subcontinent": "Caribbean"},
    "Belize": {"Continent": "North America", "Subcontinent": "Central America"},
    "Canada": {"Continent": "North America", "Subcontinent": "Northern America"},
    "Costa Rica": {"Continent": "North America", "Subcontinent": "Central America"},
    "Cuba": {"Continent": "North America", "Subcontinent": "Caribbean"},
    "Dominica": {"Continent": "North America", "Subcontinent": "Caribbean"},
    "Dominican Republic": {"Continent": "North America", "Subcontinent": "Caribbean"},
    "El Salvador": {"Continent": "North America", "Subcontinent": "Central America"},
    "Grenada": {"Continent": "North America", "Subcontinent": "Caribbean"},
    "Guatemala": {"Continent": "North America", "Subcontinent": "Central America"},
    "Haiti": {"Continent": "North America", "Subcontinent": "Caribbean"},
    "Honduras": {"Continent": "North America", "Subcontinent": "Central America"},
    "Jamaica": {"Continent": "North America", "Subcontinent": "Caribbean"},
    "Mexico": {"Continent": "North America", "Subcontinent": "Central America"},
    "Nicaragua": {"Continent": "North America", "Subcontinent": "Central America"},
    "Panama": {"Continent": "North America", "Subcontinent": "Central America"},
    "Saint Kitts and Nevis": {"Continent": "North America", "Subcontinent": "Caribbean"},
    "Saint Lucia": {"Continent": "North America", "Subcontinent": "Caribbean"},
    "Saint Vincent and the Grenadines": {"Continent": "North America", "Subcontinent": "Caribbean"},
    "Trinidad and Tobago": {"Continent": "North America", "Subcontinent": "Caribbean"},
    "United States": {"Continent": "North America", "Subcontinent": "Northern America"},

    # South America (12 countries)
    "Argentina": {"Continent": "South America", "Subcontinent": "South America"},
    "Bolivia": {"Continent": "South America", "Subcontinent": "South America"},
    "Brazil": {"Continent": "South America", "Subcontinent": "South America"},
    "Chile": {"Continent": "South America", "Subcontinent": "South America"},
    "Colombia": {"Continent": "South America", "Subcontinent": "South America"},
    "Ecuador": {"Continent": "South America", "Subcontinent": "South America"},
    "Guyana": {"Continent": "South America", "Subcontinent": "South America"},
    "Paraguay": {"Continent": "South America", "Subcontinent": "South America"},
    "Peru": {"Continent": "South America", "Subcontinent": "South America"},
    "Suriname": {"Continent": "South America", "Subcontinent": "South America"},
    "Uruguay": {"Continent": "South America", "Subcontinent": "South America"},
    "Venezuela": {"Continent": "South America", "Subcontinent": "South America"},

    # Oceania (14 countries)
    "Australia": {"Continent": "Oceania", "Subcontinent": "Australia and New Zealand"},
    "Fiji": {"Continent": "Oceania", "Subcontinent": "Melanesia"},
    "Kiribati": {"Continent": "Oceania", "Subcontinent": "Micronesia"},
    "Marshall Islands": {"Continent": "Oceania", "Subcontinent": "Micronesia"},
    "Micronesia": {"Continent": "Oceania", "Subcontinent": "Micronesia"},
    "Nauru": {"Continent": "Oceania", "Subcontinent": "Micronesia"},
    "New Zealand": {"Continent": "Oceania", "Subcontinent": "Australia and New Zealand"},
    "Palau": {"Continent": "Oceania", "Subcontinent": "Micronesia"},
    "Papua New Guinea": {"Continent": "Oceania", "Subcontinent": "Melanesia"},
    "Samoa": {"Continent": "Oceania", "Subcontinent": "Polynesia"},
    "Solomon Islands": {"Continent": "Oceania", "Subcontinent": "Melanesia"},
    "Tonga": {"Continent": "Oceania", "Subcontinent": "Polynesia"},
    "Tuvalu": {"Continent": "Oceania", "Subcontinent": "Polynesia"},
    "Vanuatu": {"Continent": "Oceania", "Subcontinent": "Melanesia"},
}

ALIASES = {
    "USA": "United States",
    "USSR": "Northern Asia",
    "Korea": "North Korea",
    "UK": "United Kingdom",
    "U.K.": "United Kingdom",
    "U.S.": "United States",
    "U.S.A.": "United States",
    "Viet Nam": "Vietnam",
    "DRC": "Democratic Republic of the Congo",

}

def normalize_country_name(country):
    """Normalize country names using aliases and lowercase matching"""
    if pd.isna(country):
        return None
        
    # Convert to string and strip whitespace
    country = str(country).strip()
    
    # Check aliases first
    if country in ALIASES:
        return ALIASES[country]
    
    # Try case-insensitive matching with COUNTRY_MAPPING
    country_lower = country.lower()
    for mapped_country in COUNTRY_MAPPING:
        if mapped_country.lower() == country_lower:
            return mapped_country
    
    # If no match found, return a normalized title-cased string.
    return normalize_title_case(country)

def extract_country(geo_location):
    """Extract country name from Geographic Location"""
    if pd.isna(geo_location) or geo_location == "absent":
        return None
    # Handle cases like "USA: New York" or "United States: California"
    raw_country = geo_location.split(":")[0].strip()
    return normalize_country_name(raw_country)

def add_geo_columns(df):
    """Add Continent and Subcontinent columns based on Geographic Location"""
    df = df.copy()
    # Extract and normalize country names
    df['Country'] = df['Geographic Location'].apply(extract_country)
    
    # Add continent and subcontinent with case-insensitive matching
    df['Continent'] = df['Country'].apply(
        lambda x: COUNTRY_MAPPING.get(x, {}).get('Continent', 'Unknown'))
    df['Subcontinent'] = df['Country'].apply(
        lambda x: COUNTRY_MAPPING.get(x, {}).get('Subcontinent', 'Unknown'))
    
    # Drop temporary Country column
    df = df.drop(columns=['Country'], errors='ignore')
    return df

def build_metadata_parser(*, add_help: bool = True) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch metadata summaries from an NCBI dataset TSV.",
        add_help=add_help,
    )
    parser.add_argument("--input", required=True, help="Path to the input TSV file")
    parser.add_argument("--outdir", required=True, help="Path to the output directory")
    parser.add_argument(
        "--sleep",
        type=float,
        default=None,
        help=(
            "Time to wait between NCBI requests. "
            f"Default is {DEFAULT_SLEEP_NO_API_KEY}s without an API key and "
            f"{DEFAULT_SLEEP_WITH_API_KEY}s with an API key."
        ),
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="NCBI API key. If omitted, fetchm will also look for NCBI_API_KEY in the environment.",
    )
    parser.add_argument(
        "--email",
        default=None,
        help="Contact email to send with NCBI E-utilities requests.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help=(
            "Number of concurrent metadata fetch workers. "
            f"Default is {DEFAULT_WORKERS_NO_API_KEY} without an API key and "
            f"{DEFAULT_WORKERS_WITH_API_KEY} with an API key."
        ),
    )
    parser.add_argument("--ani", nargs='+', choices=['OK', 'Inconclusive', 'Failed', 'all'], default=['OK'],
    help="Filter genomes by ANI status. Choices: OK, Inconclusive, Failed, all. Default is OK.")   
    parser.add_argument("--checkm", type=float, default=None,
    help="Minimum CheckM completeness threshold. If not set, no CheckM filtering will be applied.")
    parser.add_argument("--seq", action="store_true", help="Run the script to download sequences")
    add_sequence_arguments(parser, include_paths=False)
    return parser


def run_metadata_pipeline(args: argparse.Namespace) -> None:
    """Run the metadata workflow and optionally download sequences."""

    persistent_cache: Optional[MetadataPersistentCache] = None
    try:
        api_key = args.api_key or os.getenv("NCBI_API_KEY")
        effective_sleep = get_effective_sleep(args.sleep, api_key)
        effective_workers = get_effective_workers(args.workers, api_key)
        if api_key:
            logging.info(
                "Using NCBI API key with request delay %.2fs and %s workers. The key itself is not logged.",
                effective_sleep,
                effective_workers,
            )
        else:
            logging.info(
                "No NCBI API key detected. Using request delay %.2fs and %s workers.",
                effective_sleep,
                effective_workers,
            )

        # Load and filter data
        df = load_data(args.input)
        df = filter_data(df, args.checkm, args.ani)

        # Create output directories
        organism_name = df["Organism Name"].iloc[0].replace(" ", "_")
        organism_folder, metadata_folder, figures_folder, sequence_folder = create_output_directory(args.outdir, organism_name)
        cache_db_path = os.path.join(organism_folder, CACHE_DB_FILENAME)
        persistent_cache = MetadataPersistentCache(cache_db_path)

        # Fetch metadata
        df["Isolation Source"] = pd.NA
        df["Collection Date"] = pd.NA
        df["Geographic Location"] = pd.NA
        df["Host"] = pd.NA

        biosample_results = fetch_all_metadata(
            df["Assembly BioSample Accession"].tolist(),
            api_key=api_key,
            email=args.email,
            persistent_cache=persistent_cache,
            request_interval=effective_sleep,
            workers=effective_workers,
        )

        for index, row in df.iterrows():
            biosample_id = row["Assembly BioSample Accession"]
            if pd.notna(biosample_id):
                isolation_source, collection_date, geo_location, host = biosample_results.get(
                    str(biosample_id),
                    (pd.NA, pd.NA, pd.NA, pd.NA),
                )
                df.at[index, "Isolation Source"] = isolation_source
                df.at[index, "Collection Date"] = collection_date
                df.at[index, "Geographic Location"] = geo_location
                df.at[index, "Host"] = host

        # Standardize columns
        df["Isolation Source"] = df["Isolation Source"].apply(standardize_isolation_source)
        df["Collection Date"] = df["Collection Date"].apply(standardize_date)
        df["Geographic Location"] = df["Geographic Location"].apply(standardize_location)
        df["Host"] = df["Host"].apply(standardize_host)

        # Save updated data
        output_file = os.path.join(metadata_folder, "ncbi_dataset_updated.tsv")
        save_summary(df, output_file)
        
        #save metadata summary
        # Sort the DataFrame to prioritize rows with "GCF" in "Assembly Accession"
        df_sorted = df.sort_values(
            by="Assembly Accession",
            key=lambda x: x.fillna("").str.startswith("GCF"),
            ascending=False,
        )
        # Drop duplicates based on "Assembly Name", keeping the first occurrence (which will be the "GCF" row)
        df2 = df_sorted.drop_duplicates(subset=["Assembly Name"], keep="first").copy()
        output_file = os.path.join(metadata_folder, "metadata_summary.csv")
        generate_metadata_summary(df2, output_file)
        
        #save assembly summary
        output_file = os.path.join(metadata_folder, "assembly_summary.csv")
        generate_assembly_summary(df2, output_file)
        
        # Generate distribution plots for assembly columns
        for column in ["Assembly Stats Total Sequence Length"]:
            series = pd.to_numeric(df2[column], errors="coerce").dropna()
            if not series.empty:
                plot_distribution(column, series, column, figures_folder)

        # Generate and save bar plots
        for variable in ["Geographic Location", "Host", "Collection Date"]:
            frequency = df2[variable].value_counts()
            percentage = (frequency / frequency.sum()) * 100
            plot_bar_charts(variable, frequency, percentage, figures_folder)
        
        #Save map plots
        for variable in ["Geographic Location"]:
            frequency = df2[variable].value_counts()
            plot_geo_choropleth(variable, frequency, figures_folder)

        #save annotation summary
        df3 = df[df["Annotation Name"] == "NCBI Prokaryotic Genome Annotation Pipeline (PGAP)"].copy()
        output_file = os.path.join(metadata_folder, "annotation_summary.csv")
        generate_annotation_summary(df3, output_file)
        
        # Generate distribution plots for annotation columns
        for column in ["Annotation Count Gene Total", "Annotation Count Gene Protein-coding", "Annotation Count Gene Pseudogene"]:
            series = pd.to_numeric(df3[column], errors="coerce").dropna()
            if not series.empty:
                plot_distribution(column, series, column, figures_folder)

        # Filter and sort data for scatter plots
        df3_filtered = df3[df3["Collection Date"] != "absent"].copy()
        df3_filtered["Collection Date"] = pd.to_numeric(df3_filtered["Collection Date"], errors="coerce")
        df3_filtered = df3_filtered.dropna(subset=["Collection Date"])
        df3_filtered = df3_filtered.sort_values(by="Collection Date", ascending=True)
        df2_clean = df2[["Collection Date", "Assembly Stats Total Sequence Length"]].dropna()

        # Generate scatter plots with trend lines
        plot_scatter_with_trend_and_corr(
            x=df2_clean["Collection Date"],
            y=df2_clean["Assembly Stats Total Sequence Length"],
            xlabel="Collection Date",
            ylabel="Total Sequence Length",
            title="Scatter Plot: Total Sequence Length vs Collection Date",
            filename="scatter_plot_total_sequence_length_vs_collection_date.tiff",
            figures_folder=figures_folder
        )
        plot_scatter_with_trend_and_corr(
            x=df3_filtered["Collection Date"],
            y=df3_filtered["Annotation Count Gene Total"],
            xlabel="Collection Date",
            ylabel="Annotation Count Gene Total",
            title="Scatter Plot: Annotation Count Gene Total vs Collection Date",
            filename="scatter_plot_gene_total_vs_collection_date.tiff",
            figures_folder=figures_folder
        )
        plot_scatter_with_trend_and_corr(
            x=df3_filtered["Collection Date"],
            y=df3_filtered["Annotation Count Gene Protein-coding"],
            xlabel="Collection Date",
            ylabel="Annotation Count Gene Protein-coding",
            title="Scatter Plot: Annotation Count Gene Protein-coding vs Collection Date",
            filename="scatter_plot_gene_protein_coding_vs_collection_date.tiff",
            figures_folder=figures_folder
        )

        # Save clean data
        columns_to_keep = [
            "Organism Name", "Assembly BioSample Accession", "Assembly Accession", "Assembly Name", "Assembly BioProject Accession",
            "Organism Infraspecific Names Strain", "Assembly Stats Total Sequence Length",
            "Isolation Source", "Collection Date", "Geographic Location", "Host"
        ]
        # Modify your existing code:
        df4 = df2[columns_to_keep].copy()
        df4 = add_geo_columns(df4)  # Add the new columns
        clean_data_file = os.path.join(metadata_folder, "ncbi_clean.csv")
        save_clean_data(df4, columns_to_keep + ['Continent', 'Subcontinent'], clean_data_file)
        harmonization_report_file = os.path.join(metadata_folder, "metadata_harmonization_report.csv")
        generate_harmonization_report(df4, harmonization_report_file)

        # Generate and save bar plots for Continent and Subcontinent
        for variable in ["Continent", "Subcontinent"]:
            frequency = df4[variable].value_counts()
            percentage = (frequency / frequency.sum()) * 100
            plot_bar_charts(variable, frequency, percentage, figures_folder)
        
        # Check if --seq argument is provided
        if not args.seq:
            logging.info("Metadata generation completed. Sequence download not requested.")
            return

        run_sequence_downloads(
            args,
            input_path=clean_data_file,
            output_folder=sequence_folder,
        )
        logging.info("Script completed successfully.")

    except Exception as e:
        logging.error(f"Script failed: {e}")
        raise SystemExit(1) from e
    finally:
        if persistent_cache is not None:
            persistent_cache.close()


def main() -> None:
    """Main function to execute the script."""
    args = build_metadata_parser().parse_args()
    run_metadata_pipeline(args)


if __name__ == "__main__":
    main()
