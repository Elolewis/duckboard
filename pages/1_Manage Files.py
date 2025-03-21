import streamlit as st
import pandas as pd
from pathlib import Path
from helpers import data_loading as dl
import hashlib
from helpers.st_dev import developer_sidebar
developer_sidebar()


CACHE_FILE = "file_cache.json"

def validate_full_path_and_update():
    changes = st.session_state.get("ppp_table_pending_changes", {})
    ppp_table = st.session_state["ppp_table"].copy()

    # 2. Handle added rows
    new_rows = changes.get("added_rows", [])
    if new_rows:
        new_df = pd.DataFrame(new_rows)
        # Make sure all expected columns exist, fill in if missing
        for col in ppp_table.columns:
            if col not in new_df.columns:
                # Fill with empty or a default
                if col == "Validation":
                    new_df[col] = "Pending"
                else:
                    new_df[col] = ""
        new_df = new_df[ppp_table.columns]
        ppp_table = pd.concat([ppp_table, new_df], ignore_index=True)

    # 2. Update Edited Rows
    edited_rows = changes.get("edited_rows", {})
    if edited_rows:
        for idx_str, row_data in edited_rows.items():
            for col_name, new_value in row_data.items():
                ppp_table.at[int(idx_str), col_name] = new_value

                if col_name == "Full Path":
                    v_result,d_result = validate_full_path(new_value)
                    # Update the 'Validation' column based on the validation result
                    ppp_table.at[int(idx_str), "Validation"] = v_result
                    ppp_table.at[int(idx_str), "Directory"] = d_result

    # 3. Handle deleted rows
    deleted_rows = changes.get("deleted_rows", [])
    if deleted_rows:
        for row_idx in deleted_rows:
            # Make sure row_idx is valid before dropping
            if row_idx in ppp_table.index:
                # if file hash is empty, continue
                file_hash = ppp_table.loc[row_idx, "File Hash"]
                if file_hash == "":
                    continue
                else:
                    # drop from ppp_table
                    ppp_table.drop(index=row_idx, inplace=True)
                    # Also drop from the pending_parquet_partitions
                    st.session_state["pending_parquet_partitions"] = [
                        item
                        for item in st.session_state["pending_parquet_partitions"]
                        if item["File Hash"] != file_hash
                    ]
        # Reindex after deletions
        ppp_table.reset_index(drop=True, inplace=True)
    return ppp_table

def validate_full_path(full_path: str):
    _path = Path(full_path).resolve()
    if not _path or str(_path).strip() == "":
        return "Pending", ""
    elif _path.exists():
        if _path.is_dir():
            dir = _path
        else:
            dir = _path.parent
        return "Valid", str(dir)
    else:
        return "Invalid", ""

def data_needs_validation():
    """Check for changes in the data editor."""
    st.session_state['validated'] = False

def file_Upload_Section():
    st.title("File Uploader and Table Manager")

    left, right = st.columns([2, 5])
    with left:
        uploaded_file = st.file_uploader(
            "Choose a file", accept_multiple_files=False, type=["csv", "xlsx", "parquet"], key="file_uploader"
        )
        duplacate_files = []
        if uploaded_file:
            file_bytes = uploaded_file.read()
            uploaded_file.seek(0)  # Reset the file pointer to the beginning
            file_hash = hashlib.sha256(file_bytes).hexdigest()
            df, file_type, encoding = dl.data_reader(uploaded_file)
            
            payload = {
                "File Name": uploaded_file.name,
                "File Hash": file_hash,
                "File Size": uploaded_file.size,
                "File Type": file_type,
                "Encoding": encoding,
                "Type": "Temporary",
                "Location": "In-memory",
                'Validation': "Pending",
                "Path": "",
                'Alias': "",
                "Reference": "",
                "df": df,
            }

            duplicate = dl.check_membership(
                file_hash, "File Hash",
                st.session_state["available_files"],st.session_state['uploaded_files'])
            if duplicate:
                st.warning(f"NOTE: File {uploaded_file.name} has already been added previously.")
            else:
                if file_type == "parquet partition":
                    st.session_state["pending_parquet_partitions"].append(payload)

                elif isinstance(df, pd.DataFrame):
                        payload['df'] = df
                        st.session_state["available_files"].append(payload)

                elif isinstance(df, dict):
                    options=list(df.keys())
                    tabs = st.tabs(options)
                    for i, tab in enumerate(tabs):
                        with tab:
                            st.dataframe(df[options[i]].head(10))
                    
                    for j in range(len(options)):
                        sheet_payload = payload.copy()
                        sheet_payload['File Name'] = f"{uploaded_file.name}--{options[j]}"
                        sheet_payload['df'] = df[options[j]]
                        st.session_state["available_files"].append(sheet_payload)     

    with right:
        # st.session_state['uploaded_files']
        for new_file in st.session_state['available_files']:
            file_hash = new_file['File Hash']
            df = new_file['df']
            file_name = new_file['File Name']
            file_type = new_file['File Type']
            encoding = new_file['Encoding']

            with st.expander(f"File Name: '{file_name}' - File Type: '{file_type}: {encoding}'"):
                st.dataframe(df.head(10))
                if isinstance(df, dict):
                    # for xlsx with multiple sheets a dict is returned
                    st.warning(f"File {file_name} has multiple sheets. Please select sheets to load.")
                elif file_type.startswith("Error"):
                    st.error(f"FileTypeError: {file_type}") 
                elif isinstance(df, pd.DataFrame) and df.empty:
                    if file_type == "parquet partition":
                        st.warning(f"File {file_name} may be a parquet partition of a larger dataset.")
                    else:
                        st.warning(f"File {file_name} is empty or unsupported.")
                else:
                    st.success(f"File {file_name} loaded successfully.")

                        

                # show the dataframe in the expander

    # return uploaded_files



def parquet_partition_handling():
    """Handle parquet partitioning."""
    # Check if there are any pending parquet directories
    if st.session_state["pending_parquet_partitions"]:
        with st.expander(f"#### 📁 {len(st.session_state['pending_parquet_partitions'])} Pending Parquet Partitions"):
            st.write("##### Parquet Partition Handling")

            # if there is a pending parquet partition, initialize the table
            if "ppp_table" not in st.session_state:
                ppp_columns = ["File Name", "File Type", "File Size", "File Hash",'Validation', "Full Path", 'Table Name', 'Directory']
                st.session_state["ppp_table"] = pd.DataFrame(columns=ppp_columns)

            # if in debug mode, show the pending parquet partitions
            if debug_mode := st.session_state.get("debug_mode", False):
                st.write("DEBUG: session_state: ppp_table:")
                st.write("DEBUG: session_state: Pending Parquet Partitions:")
                st.write(st.session_state["pending_parquet_partitions"])

            # parse the pending parquet partitions tuple of file_id file_name into a dataframe
            selected_dirs_df = pd.DataFrame(
                st.session_state["pending_parquet_partitions"],
                columns=["File Name", "File Type", "File Size", "File Hash"]
                )
            selected_dirs_df['Validation'] = "Pending"
            selected_dirs_df[["Full Path", 'Table Name', 'Directory']] = ""
            
            st.session_state["ppp_table"] = pd.concat(
                [
                    st.session_state["ppp_table"], 
                    selected_dirs_df[~selected_dirs_df["File Hash"].isin(st.session_state["ppp_table"]["File Hash"])]
                    ],
                  ignore_index=True
                  )

            st.write(
                """These files appear to be partitons. Please enter full file path to load the entire dataset.""")
            
            st.data_editor(
                st.session_state["ppp_table"],
                use_container_width=True, 
                disabled=["File Name", 'Directory', 'Validation'], 
                column_config={
                    "Full Path": st.column_config.TextColumn(
                        help="Enter full path to file",
                        default="",
                        required=True,
                        ),
                    "Table Name": st.column_config.TextColumn(
                        help="Enter table name",
                        default="",
                        required=True,
                        ),
                    },                
                column_order=["File Name",  'Table Name', "Full Path", 'Validation'],
                num_rows='dynamic',
                key="ppp_table_pending_changes",
                on_change=data_needs_validation
                )
            colA, colB, colC = st.columns([1,3,1])
            with colA:
                if st.button("Apply and Validate"):
                    
                    st.session_state["ppp_table"] = validate_full_path_and_update()

                    if st.session_state["ppp_table"]["Validation"].eq("Valid").all():
                        st.session_state['validated'] = True
                    st.rerun()
            if st.session_state['validated']:
                with colB:
                    st.success("All paths are valid.")
                with colC:
                # Update the DataFrame with full paths
                    if st.button("Add to Tables"):
                        for _, row in st.session_state["ppp_table"].iterrows():
                            if row.to_dict() not in st.session_state["tables"]:
                                st.session_state["tables"].append(row.to_dict())
                                row.to_dict()
                        
                        # 3. Clear out the processed directories
                        st.session_state["pending_parquet_partitions"] = []
                        st.session_state["ppp_table"] = []
           
                        dl.save_cache(CACHE_FILE, st.session_state["files"], st.session_state["tables"])
                        st.rerun()


# def main():
uploaded_files = file_Upload_Section()
parquet_partition_handling()
st.subheader("Tracked Tables & Files")
# tab1, tab2, tab3 = st.tabs(["Cached Parquet Tables", "Cached Files", "Temporary Session Files"])

# st.segmented_control(
#     ["Cached Parquet Tables", "Cached Files", "Temporary Session Files"],
#     key="tab_selection",
#     label_visibility="collapsed",
#     horizontal=True,
#     default_value="Cached Parquet Tables"
# )
tab1, tab3 = st.tabs([ "Temporary Session Files", "Cached Parquet Tables",])

with tab3:
    st.write("##### Parquet Tables")
    current_data_tables = st.data_editor(
        st.session_state["tables"], 
        use_container_width=True, 
        column_order=["Table Name","Directory"],
        num_rows='dynamic'
        )
    # write to cache
    if st.button("Update Tables"):
        st.session_state["tables"] = current_data_tables.to_dict('records')
        dl.save_cache(CACHE_FILE, st.session_state["files"], st.session_state["tables"])
        st.success("Tables updated successfully.")

    else:
        st.write("No parquet tables added.")

with tab1:
    st.write("##### Session Files")
    if st.session_state["available_files"]:
        # st.write(st.session_state["session_files"])
        st.write(pd.DataFrame(st.session_state["available_files"]).columns)
        st.data_editor(st.session_state["available_files"], hide_index=True, 
                       disabled=["File Name", "File Type"],
                       column_order=["File Name", "Alias", "Path", "File Type"], num_rows='dynamic')
        if st.button("Update session files (under development)"):
            pass
    else:
        st.write("No session files added.")
