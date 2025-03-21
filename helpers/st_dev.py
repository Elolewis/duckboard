import streamlit as st

def developer_sidebar():
    with st.sidebar:
        st.title("Session State")
        if st.button("Reset session cache"):
            st.session_state["cache_loaded"] = False
            st.rerun()

        if st.button("Reset session state"):
            st.session_state.clear()
            st.rerun()
            
        st.toggle("debug mode", key="debug_mode")