
from pathlib import Path
import pandas as pd
from collections import Counter
import pyarrow.parquet as pq
import pyarrow as pa
import json
import hashlib
import itertools
import io


def load_cache(cache_file):
    """Load file paths from cache file into session state."""
    if Path(cache_file).exists():
        with open(cache_file, "r") as f:
            return json.load(f)
    return {"files": [], "tables": []}

def save_cache(cache_file, files, tables):
    """Save session state file tracking to disk."""
    cache_data = {
        "files": [{key: str(value) for key, value in file.items()} for file in files],
        "tables": [{key: str(value) for key, value in table.items()} for table in tables],
    }
    with open(cache_file, "w") as f:
        json.dump(cache_data, f, indent=4)


def get_file_hash(file):
    """Generate a hash for the file to check for duplicates."""
    hasher = hashlib.sha256()
    # Read the file's content and update the hash
    for chunk in iter(lambda: file.read(4096), b""):
        hasher.update(chunk)
    return hasher.hexdigest()

def check_membership(member, name="File Hash", *lists,):
    """
    Check if a member exists in any of the provided lists by building a set of all values for the specified key.

    Only works with dictionaries that contain the specified key.
    """
    all_lists = {
        f.get(name)  # Get value of 'name' from each dict in the lists
        for f in itertools.chain(*lists)  # chain all lists into a single iterable
        if f.get(name) is not None  # Ensure we only include non-None values
    }
    return member in all_lists

def rename_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    If any columns are duplicated, rename them by appending a suffix _1, _2, etc.
    """
    counts = Counter()
    new_cols = []
    for col in df.columns:
        counts[col] += 1
        if counts[col] > 1:
            new_cols.append(f"{col}_{counts[col]}")
        else:
            new_cols.append(col)
    df.columns = new_cols
    return df

def load_single_partition(file_bytes: bytes) -> pd.DataFrame:
    """Reads a parquet file from bytes, then renames any duplicate columns."""
    try:
        table = pq.ParquetFile(pa.BufferReader(file_bytes).read_buffer()).read()
    except Exception as e:
        raise ValueError(f"Error reading parquet file: {str(e)}")
    df = table.to_pandas()
    return rename_duplicates(df)


def data_reader(uploaded_file):
    """Read the uploaded data file and return a DataFrame along with file type."""
    file_type = uploaded_file.type
    file_bytes = uploaded_file.read()

    try:        
        if file_type in ["text/csv", "text/plain", "application/csv"]:
            encoding_type = ["ISO-8859-1", "utf-8", "latin1"]
            errors = []
            for encoding in encoding_type:
                try:
                    df = pd.read_csv(io.BytesIO(file_bytes), encoding=encoding)
                    return df, f"csv",encoding
                except Exception as e:
                    errors.append((encoding, str(e)))
                    continue
            return pd.DataFrame([]), f"Error: {errors}", ""
        elif file_type == 'application/octet-stream':
            try:
                return pd.read_parquet(uploaded_file), "parquet", ""
            except Exception as e:
                # Attempt to read as a partitioned dataset
                return pd.DataFrame([]),"parquet partition", ""
        elif file_type in ["application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "text/xlsx"]:
            xl = pd.ExcelFile(uploaded_file)
            sheet_names = xl.sheet_names
            df_dict = {}
            try:
                uploaded_file.seek(0)  # Reset file pointer to the beginning
                for sheet_name in sheet_names:
                    df_dict[sheet_name] = pd.read_excel(uploaded_file, sheet_name=sheet_name)
            except Exception as e:
                return pd.DataFrame([]), f"Error reading excel file: {str(e)}", ""
            return df_dict, "xlsx", ""
        else:
            return pd.DataFrame([]), "Unsupported file type", ""
    except Exception as e:
        return pd.DataFrame([]), f"Error: {str(e)}", ""