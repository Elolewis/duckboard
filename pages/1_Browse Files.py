import streamlit as st
import pandas as pd
import io
import json
from pathlib import Path

CACHE_FILE = "file_cache.json"

def load_cache():
    """Load file paths from cache file into session state."""
    if Path(CACHE_FILE).exists():
        with open(CACHE_FILE, "r") as f:
            return json.load(f)
    return {"files": [], "tables": [], "file_paths": {}}

def save_cache():
    """Save session state file tracking to disk."""
    cache_data = {
        "files": st.session_state["files"],
        "tables": st.session_state["tables"],
        "file_paths": st.session_state["file_paths"]
    }
    with open(CACHE_FILE, "w") as f:
        json.dump(cache_data, f, indent=4)


def data_reader(file):
    """Read the uploaded data file and return a DataFrame along with file type."""
    file_type = file.name.split(".")[-1].lower()

    try:        
        if file.type == "text/csv":
            return pd.read_csv(file, encoding="utf-8"), "csv"
        elif file_type == "parquet":
            parent_dir = file.parent
            parquet_files = list(parent_dir.glob("*.parquet"))
            if len(parquet_files) > 1:
                # Read as partitioned dataset
                return pd.read_parquet(parent_dir), "partitioned_parquet"
            else:
                return pd.read_parquet(io.BytesIO(file.read())), "parquet"
        elif file.type in ["application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "text/xlsx"]:
            return pd.read_excel(file), "xlsx"
        else:
            return pd.DataFrame([]), "Unsupported file type"
    except Exception as e:
        return pd.DataFrame([]), f"Error: {str(e)}"
    
if "tables" not in st.session_state:
    st.session_state["tables"] = []
if "files" not in st.session_state:
    st.session_state["files"] = []
if "pending_parquet_dirs" not in st.session_state:
    st.session_state["pending_parquet_dirs"] = set()  # Stores folders pending user approval
if "file_paths" not in st.session_state:
    st.session_state["file_paths"] = {}  # Stores file paths
if "parquet_files_by_dir" not in st.session_state:
    st.session_state["parquet_files_by_dir"] = {}

if "cache_loaded" not in st.session_state:
    cache_data = load_cache()
    st.session_state["files"] = cache_data["files"]
    st.session_state["tables"] = cache_data["tables"]
    st.session_state["file_paths"] = cache_data["file_paths"]
    st.session_state["cache_loaded"] = True  # Ensure cache is only loaded once

# File Upload Section
st.title("File Uploader and Table Manager")
uploaded_files = st.file_uploader(
    "Choose a file", accept_multiple_files=True,type=["csv", "xlsx", "parquet"]
)

for uploaded_file in uploaded_files:
    df, file_type = data_reader(uploaded_file)
    full_path = Path(uploaded_file.name).resolve()  # Get full path

    with st.expander(f"📂 {uploaded_file.name} - {file_type}"):
        if file_type == "Unsupported file type":
            st.warning("Unsupported file format. Please upload a CSV, XLSX, or Parquet file.")
        elif file_type.startswith("Error"):
            st.error(file_type)  # Show error message from the reader
        elif df.empty:
            st.warning(f"File {uploaded_file.name} is empty or unsupported.")
        else:
            st.success(f"File {uploaded_file.name} loaded successfully.")
            st.dataframe(df.head(10))
    
    if uploaded_file.name not in st.session_state["file_paths"]:
        st.session_state["file_paths"][uploaded_file.name] = str(full_path)

    # If it's a parquet file, store the directory → file mapping
    if file_type == "parquet":
        directory = full_path.parent.name
        st.session_state["parquet_files_by_dir"].setdefault(directory, []).append(uploaded_file.name)
        st.session_state["pending_parquet_dirs"].add(directory)


if st.session_state["pending_parquet_dirs"]:
    st.subheader("📂 Parquet Partition Handling")
    st.write("These parquet files might belong to partitioned directories. Select the parent folders you want to add as partitioned tables.")
    
    selected_dirs = st.multiselect(
        "Select directories to add as partitioned tables:",
        list(st.session_state["pending_parquet_dirs"])
    )

    if st.button("Confirm Parquet Tables"):
        # 1. Add selected directories as partitioned tables
        for directory in selected_dirs:
            if directory not in st.session_state["tables"]:
                st.session_state["tables"].append(directory)
                st.success(f"📂 {directory} added as a table.")
        
        # 2. All other directories become normal files
        unselected_dirs = st.session_state["pending_parquet_dirs"].difference(selected_dirs)
        for directory in unselected_dirs:
            files_in_dir = st.session_state["parquet_files_by_dir"].get(directory, [])
            for file_name in files_in_dir:
                if file_name not in st.session_state["files"]:
                    st.session_state["files"].append(file_name)
                    st.success(f"📄 {file_name} added to Files list (non-table parquet).")
        
        # 3. Clear out the processed directories
        st.session_state["pending_parquet_dirs"].difference_update(selected_dirs)
        # Remove both selected and unselected from parquet_files_by_dir
        for d in selected_dirs.union(unselected_dirs):
            st.session_state["parquet_files_by_dir"].pop(d, None)

        save_cache()
        st.rerun()

elif st.button("Add to Tables"):
    for uploaded_file in uploaded_files:
        file_name = uploaded_file.name
        full_path = str(Path(uploaded_file.name).resolve())

        if file_name in st.session_state["files"]:
            st.warning(f"📄 {file_name} is already in the Files list.")
        else:
            st.session_state["files"].append(file_name)
            st.session_state["file_paths"][file_name] = full_path
            st.success(f"📄 {file_name} added to the Files list.")

    save_cache()

# Display info
st.subheader("📋 Tracked Tables & Files")
col1, col2 = st.columns(2)
with col1:
    st.write("### 📂 Parquet Tables (Partitions)")
    if st.session_state["tables"]:
        for table_dir in st.session_state["tables"]:
            st.write(f"📂 {table_dir}")
    else:
        st.write("No parquet tables added.")

with col2:
    st.write("### 📄 File List")
    if st.session_state["files"]:
        for file_name in st.session_state["files"]:
            st.write(f"📄 {file_name} - `{st.session_state['file_paths'][file_name]}`")
    else:
        st.write("No files added.")
