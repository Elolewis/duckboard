import streamlit as st

def developer_sidebar():
    with st.sidebar:
        st.title("Session State")
        if st.button("Reset session"):
            st.session_state.clear()
            st.switch_page("DuckBoard.py")
            st.rerun()

        st.toggle("debug mode", key="debug_mode")