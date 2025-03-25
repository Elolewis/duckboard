import streamlit as st
import pandas as pd
from pathlib import Path
from helpers import data_loading as dl
import hashlib
from helpers.st_dev import developer_sidebar
developer_sidebar()


CACHE_FILE = "file_cache.json"
if "session_files" not in st.session_state:
    st.session_state["session_files"] = []

from typing import Optional
def validate_full_path_and_update(path_col: str = "Path", parent_drop: pd.DataFrame = None, drop_col = "File Hash") -> pd.DataFrame:
    changes = st.session_state.get("ppp_table_pending_changes", {})
    source = st.session_state["ppp_table"]
    _source = source.copy()

    # 2. Handle added rows
    new_rows = changes.get("added_rows", [])
    st.write("DEBUG: New rows:", new_rows)
    if new_rows:
        new_df = pd.DataFrame(new_rows)
        # Make sure all expected columns exist, fill in if missing
        for col in _source.columns:
            if col not in new_df.columns:
                # Fill with empty or a default
                if col == "Validation":
                    new_df[col] = "Pending"
                else:
                    new_df[col] = ""
        new_df = new_df[_source.columns]
        _source = pd.concat([_source, new_df], ignore_index=True)

    # 2. Update Edited Rows
    edited_rows = changes.get("edited_rows", {})
    if edited_rows:
        for idx_str, row_data in edited_rows.items():
            for col_name, new_value in row_data.items():
                _source.at[int(idx_str), col_name] = new_value

                if col_name == "Path":
                    valid_res,path_res, type_res = validate_full_path(new_value)
                    # Update the 'Validation' column based on the validation result
                    _source.at[int(idx_str), "Validation"] = valid_res
                    _source.at[int(idx_str), "Path"] = path_res
                    _source.at[int(idx_str), "Type"] = type_res

    # 3. Handle deleted rows
    deleted_rows = changes.get("deleted_rows", [])
    if deleted_rows:
        for row_idx in deleted_rows:
            # Make sure row_idx is valid before dropping
            if row_idx in _source.index:
                # if file hash is empty, continue
                file_hash = _source.loc[row_idx, drop_col]
                if file_hash == "":
                    continue
                else:
                    # drop from ppp_table
                    _source.drop(index=row_idx, inplace=True)
                    # Also drop from the pending_parquet_partitions
                    if parent_drop:
                        parent_drop = [
                            item
                            for item in parent_drop
                            if item[drop_col] != file_hash
                        ]
        # Reindex after deletions
        _source.reset_index(drop=True, inplace=True)
    return _source

def validate_full_path(full_path: str):
    _path = Path(full_path)
    if not full_path or str(full_path).strip() == "":
        return f"Pending", "", ""
    elif _path.exists():
        if _path.is_dir():
            path_type = "Directory"
        else:
            path_type = "File"
        return "Valid", _path.as_posix(), path_type
    else:
        return "Invalid", "", ""

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
                st.session_state["session_files"],
                st.session_state['uploaded_files'],
                st.session_state["pending_parquet_partitions"])
            
            if not duplicate:
                if file_type == "parquet partition":
                    st.session_state["pending_parquet_partitions"].append(payload)

                elif isinstance(df, pd.DataFrame):
                        payload['df'] = df
                        st.session_state["session_files"].append(payload)

                elif isinstance(df, dict):
                    options=list(df.keys())
                    # tabs = st.tabs(options)
                    # for i, tab in enumerate(tabs):
                    #     with tab:
                    #         st.dataframe(df[options[i]].head(10))
                    
                    for j in range(len(options)):
                        sheet_payload = payload.copy()
                        sheet_payload['File Name'] = f"{uploaded_file.name}--{options[j]}"
                        sheet_payload['df'] = df[options[j]]
                        st.session_state["session_files"].append(sheet_payload)     

    with right:
        # st.session_state['uploaded_files']
        for new_file in st.session_state['session_files']:
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


def parquet_partition_handling():
    """Handle parquet partitioning."""
    # Check if there are any pending parquet directories
    ppp_len = len(st.session_state["pending_parquet_partitions"])   
    if ppp_len > 0:
        with st.expander(f"#### 📁 {ppp_len} Pending Parquet Partitions"):
            st.write("##### Parquet Partition Handling")
            
            if "ppp_table" not in st.session_state:
                st.session_state["ppp_table"] = pd.DataFrame(
                    st.session_state["pending_parquet_partitions"]
                    ).head(0).copy()


            # parse the pending parquet partitions tuple of file_id file_name into a dataframe
            selected_dirs_df = pd.DataFrame(st.session_state["pending_parquet_partitions"])
            ppp_table = pd.DataFrame(st.session_state["ppp_table"]).copy()
            st.session_state["ppp_table"] = pd.concat([
                    ppp_table, selected_dirs_df[~selected_dirs_df["File Hash"].isin(ppp_table["File Hash"])]
                    ],
                  ignore_index=True
                  )

            st.write("""These files appear to be partitons. Please enter full file path to load the entire dataset.""")
            
            st.data_editor(
                st.session_state["ppp_table"],
                use_container_width=True, 
                disabled=["File Name", 'Validation'], 
                column_config={
                    "Path": st.column_config.TextColumn(
                        help="Enter full path to file or parent directory",
                        default="",
                        required=True,
                        ),
                    "Alias": st.column_config.TextColumn(
                        help="Enter table name",
                        default="",
                        required=True,
                        ),
                    },                
                column_order=["File Name",  'Alias', "Path", 'Validation'],
                num_rows='dynamic',
                key="ppp_table_pending_changes",
                on_change=data_needs_validation
                )
            
            colA, colB, colC = st.columns([1,3,1])
            if any(bool(v) for v in st.session_state['ppp_table_pending_changes'].values()):
                with colA:
                    if st.button("Validate changes"):
                        try:
                            st.session_state["ppp_table"] = validate_full_path_and_update()
                            # st.dataframe(st.session_state["ppp_table"])

                            if st.session_state["ppp_table"]["Validation"].eq("Valid").all():
                                st.session_state['validated'] = True
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error validating paths: {e}")
                            st.session_state['validated'] = False
            
            if st.session_state['validated']:
                with colB:
                        st.success("All paths are valid.")
                with colC:
                            # Update the DataFrame with full paths
                    if st.button("Add to Tables"):
                        for _, row in pd.DataFrame(st.session_state["ppp_table"]).iterrows():

                            if row.to_dict() not in st.session_state["tables"]:
                                st.session_state["tables"].append(row.to_dict())
                                row.to_dict()

                            row_hash = row['File Hash']

                            # remove the row from pending parquet partitions
                            st.session_state["pending_parquet_partitions"] = [
                                item for item in st.session_state["pending_parquet_partitions"]
                                if item["File Hash"] != row_hash
                            ]
                        # 1. Save the updated tables to the cache                
                            # 3. Clear out the processed directories

                        st.session_state["pending_parquet_partitions"] = []
                        st.session_state["ppp_table"] = st.session_state["ppp_table"].head(0).copy()

                        dl.save_cache(CACHE_FILE, st.session_state["tables"])
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
# tab1, tab3 = st.tabs([ "Temporary Session Files", "Cached Parquet Tables",])

# with tab3:
#     st.write("##### Parquet Tables")
#     current_data_tables = st.data_editor(
#         st.session_state["tables"], 
#         use_container_width=True, 
#         column_order=["Table Name","Directory"],
#         num_rows='dynamic'
#         )
#     # write to cache
#     if st.button("Update Tables"):
#         st.session_state["tables"] = current_data_tables.to_dict('records')
#         dl.save_cache(CACHE_FILE, st.session_state["files"], st.session_state["tables"])
#         st.success("Tables updated successfully.")

#     else:
#         st.write("No parquet tables added.")

# with tab1:
st.write("##### Session Files")
st.session_state["available_files"] = st.session_state["session_files"] + st.session_state["tables"]

if st.session_state["available_files"]:
    column_order=["File Name", "File Type", "Alias", "Path"]
    st.write("Add file name and path to save files to cache. You can also set an alias for a file or table.")

    st.toggle("Session Files", key="toggle_session_files")
    if st.session_state["toggle_session_files"]:
        all_files = pd.DataFrame(
            st.data_editor(st.session_state["available_files"], hide_index=False,
            disabled=["File Name", "File Type"], use_container_width=True,
            column_order=column_order, num_rows='dynamic'))
        
        if st.button("Update session files"):
            for idx, row in all_files.iterrows():
                valid_res, path_res, type_res = validate_full_path((row["Path"]))
                all_files.at[idx, "Validation"] = valid_res
                all_files.at[idx, "Path"] = path_res
                all_files.at[idx, "Type"] = type_res

                
                if type_res == "File":
                    all_files.at[idx, "Reference"] = f"read_{row["File Type"]}('{path_res}')"
                elif type_res == "Directory":
                    dir_path = (Path(path_res) / "*.parquet").resolve().as_posix()
                    all_files.at[idx, "Reference"] = f"read_parquet('{dir_path}')"
                else:
                    if row["Alias"] == "":
                        all_files.at[idx, "Alias"] = row["File Name"]
                    all_files.at[idx, "Reference"] = row["Alias"]

                if valid_res == "Valid":
                    all_files.at[idx, "df"] = ""

            st.session_state["available_files"] = all_files.to_dict('records')
            file_paths_df = all_files[all_files["Validation"] == "Valid"]
            if not file_paths_df.empty:

                dl.save_cache(CACHE_FILE, file_paths_df.to_dict('records'))

                st.session_state["tables"] = dl.load_cache(cache_file=CACHE_FILE)["tables"]
                st.session_state["session_files"] = [
                    item for item in st.session_state["session_files"]
                    if item["File Hash"] not in file_paths_df["File Hash"].values
                ]

                st.success("Session files updated successfully.")
                st.rerun()
                    


    else:
        st.dataframe(
            pd.DataFrame(st.session_state["available_files"]),
            use_container_width=True,column_order=column_order,
        )
else:
    st.write("No session files added.")

st.dataframe(st.session_state["available_files"], use_container_width=True)