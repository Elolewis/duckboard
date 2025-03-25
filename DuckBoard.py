import streamlit as st
from helpers import data_loading as dl
# from helpers.st_dev import developer_sidebar

st.set_page_config(page_title="Duckboard", page_icon="assets/duckboard.ico.png", layout="centered")


CACHE_FILE = "file_cache.json"
with st.sidebar:
    st.image("./assets/duckboard.ico.png")

# st.session_state

if cache_loaded := st.session_state.get("cache_loaded", False) is False:
    cache = dl.load_cache(cache_file=CACHE_FILE)

    st.session_state["saved_queries"] = dl.load_queries_from_cache("query_cache.json")
    st.session_state["tables"] = cache["tables"]
    st.session_state["pending_parquet_partitions"] = []
    st.session_state["pending_files"] = []
    st.session_state["available_files"] = []
    st.session_state['uploaded_files'] = [] 

    st.session_state["session_files"] = []    
    st.session_state["loaded_file_dfs"] = []

    st.session_state['validated'] = False
    st.session_state['debug_mode'] = False
    st.session_state["cache_loaded"] = True  # Ensure cache is only loaded once

# developer_sidebar()

st.title("🦆 Duckboard")
st.subheader("Your local SQL workspace for structured data analysis")

st.markdown("""
Duckboard is a lightweight interface for exploring datasets, building queries, and running custom Python scripts on local files — all without a database.

**What you can do:**
- 🗂 **Manage & Load Data**: Upload CSV, Excel, or Parquet files, view schemas, assign aliases, and preview content.
- 📊 **Query Your Data**: Write SQL using table references or subquery templates. Supports DuckDB functions and file-backed tables.
- 🧩 **Run Custom Scripts**: Apply custom Python scripts using your loaded data as inputs. Great for transformations and visualizations.
""")

st.divider()

st.markdown("### 🔎 Need Help?")

with st.expander("📖 Getting Started Guide"):
    st.markdown("""
    1. Go to **Manage Data** to upload or view files.
    2. Use **Query Data** to write SQL with aliases like `{{sales_2023}}`.
    3. Add **Saved Queries** as reusable subqueries.
    4. Head to **Custom Scripts** to manipulate or visualize data using Python.

    Use the sidebar to navigate between pages.
    """)

with st.expander("📚 SQL Templating Tips"):
    st.markdown("""
    - Use `{{alias}}` to insert a data reference or subquery.
    - Example:
        ```sql
        SELECT * FROM {{sales_data}} JOIN {{customers}} USING (customer_id)
        ```
    - Saved queries are automatically wrapped in parentheses if needed.
    """)

with st.expander("📁 Supported File Types"):
    st.markdown("- CSV (.csv)\n- Excel (.xlsx)\n- Parquet (.parquet)")

st.caption("Built with ❤️ using DuckDB + Streamlit")