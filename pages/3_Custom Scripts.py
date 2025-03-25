import importlib.util
import sys
import os
import io
import pandas as pd
import streamlit as st
from datetime import datetime

if "user_pages" not in st.session_state:
    st.session_state.user_pages = {}

CACHE_DIR = 'user_cache'  # Directory to save uploaded files
os.makedirs(CACHE_DIR, exist_ok=True)

def load_user_page(page_name, file_path):
    """Load a user-defined page from uploaded file content."""
    module_name = f"user_page_{page_name}"

    # Save the uploaded file content to a temporary file
    # Load the module dynamically
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception as e:
        st.error(f"Error loading module {module_name}: {e}")
        return
    
    # Check if the module has a `show()` function
    if hasattr(module, 'show'):
        st.session_state.user_pages[page_name] = module.show

def save_uploaded_file(uploaded_file, page_name):
    """Save uploaded file to the local cache."""
    file_path = os.path.join(CACHE_DIR, f"{page_name}.py")
    with open(file_path, 'wb') as f:
        f.write(uploaded_file.getbuffer())
    return file_path

def get_cached_files():
    """Retrieve all cached files."""
    files = [f[:-3] for f in os.listdir(CACHE_DIR) if f.endswith('.py')]
    return files

def load_all_cached_files():
    """Load all files from the cache directory into session state."""
    cached_files = get_cached_files()
    for file_name in cached_files:
        file_path = os.path.join(CACHE_DIR, f"{file_name}.py")
        load_user_page(file_name, file_path)

# Load all cached files at startup
load_all_cached_files()

st.title("Custom Streamlit Scripts")
script_names = ["[ Script Management ]"] + list(st.session_state.user_pages.keys())
page =st.selectbox("Select a script", script_names)

if page != "[ Script Management ]":
    if st.button(f"Run {page}"):
        st.session_state.user_pages[page]()  # Execute script only when button is clicked
else:
    tab1, tab2, tab3, tab4 = st.tabs(["Upload Scripts", "Write Scripts", "Edit Scripts", "Manage Cached Scripts"])

    # Tab 1: Upload Scripts
    with tab1:
        st.title("Load Custom Scripts")
        st.write("Upload your custom Python scripts here.")

        # Upload section
        uploaded_file = st.file_uploader("Upload your .py file", type="py")
        if uploaded_file:
            page_name = st.text_input("Enter a name for your page", value="Custom Page")
            if st.button("Add Page"):
                file_path = save_uploaded_file(uploaded_file, page_name)
                load_user_page(page_name, file_path)
                st.success(f"Page '{page_name}' added successfully!")


    # Tab 2: Write New Scripts
    with tab2:
        st.title("Write Custom Scripts")
        st.write("Write your custom Python scripts here.")

        # Text area for script input
        page_name = st.text_input("Enter a name for your page", value="Custom Page")
        script_content = st.text_area("Enter your Python script", height=300)
        if st.button("Save Script"):
            with open(os.path.join(CACHE_DIR, f"{page_name}.py"), 'w') as f:
                f.write(script_content)
            load_user_page(page_name, io.BytesIO(script_content.encode()))
            st.success(f"Page '{page_name}' added successfully!")
            st.rerun()

    # Tab 3: Edit Existing Scripts
    with tab3:
        st.title("Edit Custom Scripts")
        st.write("Edit your custom Python scripts here.")
        script_files = [f for f in os.listdir(CACHE_DIR) if f.endswith('.py')]
        selected_file = st.selectbox("Select a script to edit", script_files)
        if selected_file:
            with open(os.path.join(CACHE_DIR, selected_file), 'r') as f:
                script_content = f.read()
            new_content = st.text_area("Edit your Python script", value=script_content, height=300)
            if st.button("Save Changes"):
                with open(os.path.join(CACHE_DIR, selected_file), 'w') as f:
                    f.write(new_content)
                load_user_page(selected_file[:-3], os.path.join(CACHE_DIR, selected_file))
                st.success(f"Changes to '{selected_file}' saved successfully!")
                st.rerun()

    # Tab 4: Manage Cached Scripts
    with tab4:
        st.title("Manage Cached Scripts")
        st.write("View or delete cached scripts from the local cache directory.")
        
        cached_files = get_cached_files()
        
        if not cached_files:
            st.write("No cached scripts found.")
        else:
            table_data = []

            for script in cached_files:
                file_path = os.path.join(CACHE_DIR, f"{script}.py")
                file_stats = os.stat(file_path)

                table_data.append({
                    "Script Name": script,
                    "Created Date": datetime.fromtimestamp(file_stats.st_ctime).strftime('%Y-%m-%d %H:%M:%S'),
                    "Modified Date": datetime.fromtimestamp(file_stats.st_mtime).strftime('%Y-%m-%d %H:%M:%S'),
                    "Source Path": file_path if os.path.exists(file_path) else "write",
                    "File Size": f"{file_stats.st_size / 1024:.2f} KB"
                })
            
            table_df = pd.DataFrame(table_data)
            cache_table = st.dataframe(
                table_df,
                use_container_width=True,
                hide_index=True,
                on_select="rerun",
                selection_mode="multi-row",
            )
            selected_index = cache_table["selection"]['rows']
            selected_scripts = (table_df.iloc[index] for index in selected_index)

            # text box that user mus type delete in order to delete the selected scripts
            delete_confirmation = st.text_input("Type 'DELETE' to confirm deletion of selected scripts")

            if delete_confirmation == "DELETE":
                if st.button("Confirm Delete"):
                    for script in selected_scripts:
                        script_name = script["Script Name"]
                        file_path = os.path.join(CACHE_DIR, f"{script_name}.py")

                        if os.path.exists(file_path):
                            os.remove(file_path)
                            st.session_state.user_pages.pop(script_name, None)
                            st.success(f"Deleted '{script_name}' from cache.")
                        else:
                            st.error(f"'{script_name}' not found in cache.")
                    st.rerun()


