import streamlit as st
import pandas as pd
import duckdb
from pathlib import Path

from helpers.st_dev import developer_sidebar
developer_sidebar()

options = {
    "Current session files": "session_files",
    "Cached files": "files",
    "Parquet tables": "tables"
}



with st.expander("Available to query", expanded=True):
    st.write("#### Available files and tables to query")
    st.write("This is a list of files and tables available for querying.")

    st.session_state["available_files"] = []
    # for file name, dt in st.session_state["session files"] append to available files
    
    for file in st.session_state["session_files"]:
        values = {
            "Alias": file["File Name"],
            "Type": "Temporary",
            "Location": "Memory",
            "FROM": file["File Name"],
        }
        st.session_state["available_files"].append(values)

    for file in st.session_state["files"]:
        values = {
            "Alias": file["File Name"],
            "Type": "File",
            "Location": file["File Path"],
            "FROM": f"read_{Path('File Path').suffix[1:]}('$Location')",
        }
        st.session_state["available_files"].append(values)

    for file in st.session_state["tables"]:
        values = {
            "Name": file["Table Name"],
            "Type": f"Directory",
            "Location": file["Directory"],
            "FROM": f"read_parquet('$Location'/*.parquet)",
        }
        st.session_state["available_files"].append(values)
        
    st.dataframe(st.session_state["available_files"])

    # Display the dataframes in a multi-select box
    # selected_source = st.selectbox("Select a source", options.keys(), key="source_select")
    # source_val = st.session_state.get(options[selected_source], [])

with st.expander("Preview", expanded=True):

    selected_source = st.selectbox("select a source", options.keys(),key="source_select")
    source_val = st.session_state.get(options[selected_source], [])

    if debug_mode := st.session_state.get("debug_mode", False):
        st.write("DEBUG: Selected source:", selected_source)
        st.write("DEBUG: Selected source value:", options[selected_source])

    if source_val:
        selected_row = st.dataframe(
            source_val, 
            selection_mode="single-row", 
            hide_index=True, 
            on_select="rerun", 
            column_order=["Table Name", "Directory", "File Name", "File Path"]
            )
    else:
        st.write("No files or tables available in the selected source.")
        selected_row = None


    if selected_row is not None:
        if debug_mode:
            st.write("DEBUG: Selected row:", selected_row)
            st.write("DEBUG: Selected row id:", selected_row['selection']["rows"][0])
            st.write(f"DEBUG: Selected row data:")
            
            # write all values except df

            keys = list(source_val[0].keys()-['df'])
            key_values = {key: source_val[0][key] for key in keys}
            st.write("DEBUG: Key values:", key_values)

        method_map = {
            'csv': {"method":'read_csv'},
            'parquet': {"method":'read_parquet'},
            'json': {"method":'read_json'},
            'excel': {"method":'read_excel}'}
        }
        tab1, tab2 = st.tabs(["Schema", "Data Preview"])

        if selected_source == "Cached files":
            df_path = Path(source_val[0]['File Path'])
            ext = df_path.suffix[1:]
            name = df_path.stem
            method = method_map.get(ext, None)
            df_name = f"df_{source_val}_{name}"
            query = f"SELECT * FROM {method[method]}('{df_path}' );"

            if method is None:
                st.error(f"Unsupported file type: {ext}")
            
            if debug_mode:
                st.write("DEBUG: File extension:", ext)
                st.write("DEBUG: File name:", name)
                st.write("DEBUG: Query to execute:", query)

            if ext == "parquet":
                from_reference = f"read_parquet('{df_path}')"
            elif st.session_state.get(df_name) is not None:
                from_reference = st.session_state[df_name]
            else:
                if st.button("Click to load file"):
                    try:
                        load_df = duckdb.sql(query).to_df()
                        st.session_state[df_name] = load_df
                        st.session_state["loaded_file_dfs"].append(df_name)
                        st.success(f"File loaded successfully: {df_name}")
                    except Exception as e:
                        st.error(f"Error reading ext file: {e}")
                with tab1:
                    st.write("file not loaded yet")
                with tab2:
                    st.write("file not loaded yet")
        else:
            if selected_source == "Current session files":
                session_file = source_val[0]['df']
                from_reference = "session_file"
            else:
                df_dir = source_val[0]['Directory']
                from_reference = f"read_parquet('{df_dir}/*.parquet')"
                

        sample_query = f"SELECT * FROM {from_reference} LIMIT 10;"
        schema_query = f"DESCRIBE {sample_query}"
        with tab1:
            st.write("#### Schema")
            try:
                schema = duckdb.sql(schema_query).to_df()
                if schema.empty:
                    st.write("No schema available.")
                else:
                    st.dataframe(schema, hide_index=True, use_container_width=True)
            except Exception as e:
                st.error(f"Error reading schema: {e}")

        with tab2:
            st.write("#### Data Preview")
            try:
                
                df = duckdb.sql(sample_query).to_df()
                if df.empty:
                    st.write("No data available.")
                else:
                    st.dataframe(df.head(), hide_index=True, use_container_width=True)
            except Exception as e:
                st.error(f"Error reading data: {e}")
            if debug_mode:
                st.write("DEBUG: query executed: {query}")

st.code(
    """
    SELECT * FROM read_parquet('path/to/file.parquet') LIMIT 10;
    """,
    language="sql",
)


