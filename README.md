# 🦆 Duckboard

**A local SQL workspace for structured data analysis — no database, no server, no vendor.**

Duckboard is a desktop application for querying local data files with SQL. Point it at CSV, Excel,
or Parquet files, give them aliases, and query them with DuckDB. It ships as a native application
(`.app`, `.exe`, `.dmg`), so an analyst can run it without a Python environment, a server, or an
administrator.

## Why it's built this way

Traditional BI tools earn their overhead when you need access controls on data and views, or broad
sharing across an organization. For exploratory analysis on file-based data they add dashboard
development effort, constrain the plots you can actually build, and tend to hide the qualitative
detail underneath. Duckboard keeps the analyst in SQL and Python, where the expressiveness is.

- **Files are the source of truth.** Parquet, CSV, and Excel are read in place. No ingest step, no
  warehouse, nothing to sync. Partitioned Parquet datasets are supported directly, so a partition
  layout designed for efficient reuse stays useful here.
- **DuckDB does the work.** Files are registered as DuckDB views, so the full DuckDB SQL dialect and
  its functions are available against local data with no server process.
- **No vendor lock-in.** Everything is open-source and local. Data never leaves the machine, which
  also makes it viable where uploading to a cloud tool isn't permitted.
- **Python where SQL runs out.** Custom Python scripts load as first-class pages, so transformations
  and visualizations a dashboard tool can't express are just code.

## Features

### 📁 Manage & load data
Upload or reference CSV, Excel, and Parquet files. View schemas, assign query aliases, preview
contents, and register partitioned Parquet datasets. Files are content-hashed to detect duplicates,
and the working set is cached between sessions.

### 📊 Query data
Write SQL against your files using `{{alias}}` references:

```sql
SELECT * FROM {{sales_data}} JOIN {{customers}} USING (customer_id)
```

Saved queries can be referenced by alias too, and are automatically wrapped as subqueries — so a
query becomes a reusable building block without creating a table or a view in a database.

### 🧩 Custom scripts
Drop in a Python file that defines a `show()` function and it becomes a page in the app, with your
loaded data available as input. Write scripts in the app or upload them; cached scripts reload on
start. This is the escape hatch for anything SQL and standard charts can't do.

## Run it directly

Requires Python with Streamlit and DuckDB installed.

```bash
streamlit run DuckBoard.py
```

## Build a desktop application

The build wraps the Streamlit app with the Pyodide runtime inside Electron and packages it with
electron-builder. Requires Node.js.

```bash
npm install        # installs packages into node_packages
npm run dump       # creates ./build with app files, dependencies, Pyodide, and Electron
npm run serve      # launches Electron against ./build/electron/main.js
npm run app:dist   # bundles ./build into .app / .exe / .dmg in ./dist
```

To customize the packaged application (icon and similar), follow the
[electron-builder](https://www.electron.build/) instructions.

## Project layout
