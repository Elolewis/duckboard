import streamlit as st
import pandas as pd

if "tables" not in st.session_state:
    st.session_state["tables"] = []
if "files" not in st.session_state:
    st.session_state["files"] = []

with st.expander("Parquet Tables"):
    st.session_state["tables"]
with st.expander("Files"):
    st.session_state["files"]