import streamlit as st
import pandas as pd
import duckdb
import io

def data_reader(file):
    """Read the data from the file."""
    file_type = file.name.split(".")[-1].lower()

    if file.type == "text/csv":
        return pd.read_csv(file), "csv"
    elif file.type == "text/xlsx":
        return pd.read_excel(file), "xlsx"
    elif file_type == "parquet":
        return duckdb.read_parquet(buffer), "parquet"
    else:
        return pd.DataFrame([]), "Unsupported file type"
    
if "tables" not in st.session_state:
    st.session_state["tables"] = []
if "files" not in st.session_state:
    st.session_state["files"] = []

cont = st.container()
uploaded_files = st.file_uploader(
    "Choose a file", accept_multiple_files=True
)

for uploaded_file in uploaded_files:
    df, type = data_reader(uploaded_file)
    with cont.expander(f"{uploaded_file.name} - {type}"):
        st.dataframe(df.head(10))

if st.button("Add to tables"):
    for uploaded_file in uploaded_files:
        if uploaded_file.type == "application/octet-stream":
            # pull the entire directory
            directory = uploaded_file.name.split("/")[0]
            if directory in st.session_state["tables"]:
                st.warning(f"{directory} already in the Tables list")
            else:
                st.session_state["tables"].append(directory)
                st.success(f"{directory} added to the Tables list")
        else:
            if uploaded_file.name in st.session_state["files"]:
                st.warning(f"{uploaded_file.name} already in the Files list")
            else:
            # add the file to session state
                st.session_state["files"].append(uploaded_file.name)
                st.success(f"{uploaded_file} added to the Files list")

st.write("""
    if data is in a parquet file, the entire directory containing
    the sample file will be added to the list of parquet tables
    """)