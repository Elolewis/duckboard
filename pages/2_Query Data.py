import streamlit as st
import pandas as pd
import duckdb
import re
from helpers import data_loading as dl

cache_file = "query_cache.json"
from helpers.st_dev import developer_sidebar
# developer_sidebar()

options = {
    "Current session files": "session_files",
    "Cached files": "files",
    "Parquet tables": "tables"
}

def register_views_in_duckdb(available_files):
    for file_info in available_files:
        alias = file_info["Alias"]
        reference = file_info["Reference"]

        create_view_query = f"CREATE OR REPLACE VIEW {alias} AS SELECT * FROM {reference};"
        duckdb.sql(create_view_query)

def expand_sql_query(user_query: str, data_aliases: dict, saved_queries: dict) -> str:
    """
    Replace {{alias}} references in a SQL query using data sources and subqueries.
    
    Args:
        user_query (str): SQL query containing {{alias}} references
        data_aliases (dict): alias → table/view reference (e.g., read_parquet(...))
        saved_queries (dict): alias → subquery SQL (will be wrapped in parentheses)
    
    Returns:
        str: Expanded SQL query with all {{alias}} replaced
    """
    alias_pattern = re.compile(r"\{\{([\w\-]+)\}\}")  # Matches {{alias_name}}

    def resolve_alias(alias: str) -> str:
        if alias in data_aliases:
            return data_aliases[alias]
        elif alias in saved_queries:
            subquery = saved_queries[alias]
            return subquery if subquery.strip().startswith("(") else f"({subquery})"
        else:
            raise ValueError(f"Unknown alias '{{{{{alias}}}}}' in query.")

    expanded_query = user_query

    for match in alias_pattern.findall(user_query):
        replacement = resolve_alias(match)
        expanded_query = expanded_query.replace(f"{{{{{match}}}}}", replacement)

    return expanded_query


if "saved_queries" not in st.session_state:
    st.session_state["saved_queries"] = {}
if "query_result_df" not in st.session_state:
    st.session_state["query_result_df"] = pd.DataFrame()
st.session_state["available_files"] = st.session_state["session_files"] + st.session_state["tables"]
register_views_in_duckdb(st.session_state["available_files"])

tab1, tab2, tab3 = st.tabs(["Query Data", "Saved Queries", "Available Tables"])
with tab3:

    st.write("#### Available files and tables to query")
    st.write("This is a list of files and tables available for querying.")

    selected_view = st.dataframe(st.session_state["available_files"], column_order=["File Name", "File Type", "Alias", "Path", "Type"], hide_index=True, use_container_width=True, selection_mode="single-row", on_select="rerun")
    
    selected_view_rows = selected_view["selection"]["rows"]

    
    if selected_view_rows:
        row = selected_view_rows[0]
        data = st.session_state["available_files"][row]



        try:
            ref = data['Reference']
            sample_query = f"SELECT * FROM {ref} LIMIT 100;"
            preview_df = duckdb.sql(sample_query).to_df()
        except Exception as e:
            # st.error(f"Error executing query: {e}")
            if data['File Type'] == "csv":
                try:
                    ref= f"sniff_csv('{data['Path']}')"
                    sample_query = f"SELECT * FROM {ref} LIMIT 100;"
                    preview_df = duckdb.sql(sample_query).to_df()
                except Exception as e:
                    ref = f"read_csv_auto('{data['Path']}')"
                    sample_query = f"SELECT * FROM {ref} LIMIT 100;"
                    preview_df = duckdb.sql(sample_query).to_df()
        
        schema_query = f"DESCRIBE {sample_query}"
        
        with st.expander("Available to query", expanded=False):
            st.write(data, hide_index=True, use_container_width=True, column_order=["File Name", "File Type", "Alias", "Path", "Type"])

        with st.expander("Schema", expanded=False):
            st.dataframe(
                duckdb.sql(schema_query).to_df(),
                hide_index=True,
                use_container_width=True,
                )
        with st.expander("Data Preview - First 100 rows", expanded=True):

            st.dataframe(
                preview_df,
                hide_index=True,
                use_container_width=True,
                )

    # TODO 
    # verify all Aliases are unique. 
    # add editing functionality to change the Alias name.
    # store available files directly instead of files and tables separately

    # Register views in DuckDB for each file

    # Display the dataframes in a multi-select box
    # selected_source = st.selectbox("Select a source", options.keys(), key="source_select")
    # source_val = st.session_state.get(options[selected_source], [])


with tab2:
    st.write(st.session_state["saved_queries"])


with tab1:
    default_query = "SELECT * FROM {{alias}} LIMIT 10;" 
    user_query = st.text_area("Enter your SQL query", value=default_query, height=150)

    # replace all aliases with their references
    data_aliases = {
        file["Alias"]: file["Reference"]
        for file in st.session_state["available_files"]
        if file.get("Alias") and file.get("Reference")
    }
    try:
        adj_user_query = expand_sql_query(user_query,data_aliases, st.session_state["saved_queries"])
    except ValueError as e:
        adj_user_query = user_query
        # adj_user_query = adj_user_query.replace(value, reference)
    st.code(
        adj_user_query,
        language="sql",
    )

    run_button_col, name_text_col, save_button_col = st.columns([1,3,1])

    with run_button_col:
        if st.button("Run Query"):
            st.session_state["query_result_df"] = duckdb.sql(user_query).to_df()
            st.rerun()

        if st.button("export query", help="Export the query to a file format"):
            pass

    with st.expander("#### Query Results", expanded=True):
        if st.session_state["query_result_df"].empty:
            st.write("No data returned.")
        else:
            st.dataframe(st.session_state["query_result_df"], hide_index=True, use_container_width=True)
    
    with name_text_col:
        st.text_input("query_name", help = "(Optional) Enter a name for the saved query", key="saved_query_name")
        query_name = st.session_state["saved_query_name"]

        if query_name in st.session_state["saved_queries"]:
            st.warning(f"Caution: this will overwriting an existing query '{query_name}'")


    with save_button_col:
        save_query_button = st.button("Save Query")
        st.session_state["save_query_to_cache"] = st.checkbox("Save query to cache", value=False)
        if save_query_button:
            if st.session_state["saved_query_name"]:
                # same query as key value with the name, query
                # adj_user_query = user_query.replace(alias, reference)
                
                
                st.session_state["saved_queries"][st.session_state["saved_query_name"]] = adj_user_query
                if st.session_state["save_query_to_cache"]:
                    dl.save_queries_to_cache(st.session_state["saved_queries"], cache_file)
                st.success("Query saved successfully.")
                st.rerun()
            else:
                st.warning("No query to save.")

