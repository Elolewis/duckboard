import streamlit as st
from helpers import data_loading as dl
from helpers.st_dev import developer_sidebar

st.set_page_config(layout="wide")


CACHE_FILE = "file_cache.json"


# st.session_state

if cache_loaded := st.session_state.get("cache_loaded", False) is False:
    cache = dl.load_cache(cache_file=CACHE_FILE)

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

developer_sidebar()
st.title("Streamlit in Electron")
st.write("This is a Streamlit app running inside an Electron window.")

if st.session_state["debug_mode"]: st.write(st.session_state)

