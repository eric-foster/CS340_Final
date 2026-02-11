# CS340 Final — MongoDB CRUD + Dash Rescue Dashboard

A Python Dash dashboard backed by MongoDB that allows users to explore an animal shelter dataset, apply rescue-type filters, visualize breed distribution, and view animal locations on an interactive map.

This repository includes:
- A **MongoDB CRUD module** (`DB_CRUD_Python_Module.py`) used as the data access layer
- A **Dash/JupyterDash dashboard notebook** (`Dashboard.ipynb`) that renders the UI (table + chart + map)
- Enhancements focused on **security, scalability, and maintainability** (environment-based secrets, projection + pagination, mapping-based filters, indexes, and server-side aggregation)

---

## Features

### Dashboard UI
- **Filter controls** for rescue types:
  - Water Rescue
  - Mountain Rescue
  - Disaster Rescue
  - Reset (no filter)
- **Interactive table** (sorting/filtering supported in the UI)
- **Breed distribution chart** (pie chart)
- **Geolocation map** (marker + tooltip/popup based on selected row)

### Database Layer (CRUD Module)
- Create, read, update, delete operations for the `animals` collection
- Enhanced reads that support:
  - **Projection** (return only the fields the dashboard needs)
  - **Pagination** via `limit` and `skip`
  - Optional sorting
- **Compound indexes** created at startup for faster filtering queries
- **Aggregation pipeline** for scalable "breed count" analytics (server-side)

---

## Enhancements Implemented (What Changed and Why)

### 1) Environment-based secrets (removes hardcoded credentials)
**Why:** Hardcoding usernames/passwords is insecure and makes deployment harder.  
**What changed:** Dashboard reads MongoDB connection values from environment variables.

Recommended `.env` keys:
```bash
MONGO_USER=yourAdminUsername
MONGO_PASS=yourStrongPassword
MONGO_HOST=127.0.0.1
MONGO_PORT=27017
MONGO_DB=aac
MONGO_COL=animals
MONGO_AUTHSOURCE=aac
```

### 2) Projection + pagination in `read()`
**Why:** Pulling entire documents (and all records) is slow and memory-heavy.  
**What changed:** The CRUD `read()` supports `projection`, `limit`, `skip`, and `sort`, allowing the dashboard to fetch only what it needs.

### 3) Mapping-based filters
**Why:** A dictionary-based mapping is cleaner and easier to extend than long `if/elif` chains.  
**What changed:** Rescue filters are stored in `FILTER_QUERIES` and selected with `FILTER_QUERIES.get(filter_type, {})`.

### 4) Compound indexes for filter patterns
**Why:** Queries that repeatedly filter on the same fields benefit significantly from indexing.  
**What changed:** `ensure_indexes()` creates indexes aligned with the dashboard’s filter usage (safe to call repeatedly).

### 5) Server-side aggregation for analytics
**Why:** Computing breed counts in Python from the table view does not scale.  
**What changed:** `breed_counts()` uses a MongoDB aggregation pipeline to compute breed totals for the selected filter.

---

## Tech Stack

- **Python:** 3.10.11 (required)
- **MongoDB:** local instance (default port 27017)
- **Dash / JupyterDash:** dashboard UI
- **PyMongo:** database access
- **dash-leaflet:** interactive map
- **pandas / plotly:** data handling + charts

---

## Repository Structure (Typical)

> Filenames may vary slightly depending on your working copy, but the key artifacts are:

- `Dashboard.ipynb` — JupyterDash dashboard notebook
- `DB_CRUD_Python_Module.py` — MongoDB CRUD + enhancements
- `requirements.txt` — Python dependencies
- `aac_shelter_outcomes.csv` — source dataset (used for import)
- `Grazioso Salvare Logo.png` — dashboard branding asset

---

## Clone the Repository

```bash
git clone https://github.com/eric-foster/Animal_Shelter_Dashboard.git
cd Animal_Shelter_Dashboard
```

---

## Setup and Run (Start-to-Finish)

### 1) Python version requirement
- **Python 3.10.11** is required for this setup.

### 2) Create a virtual environment (Python 3.10)
From the repository root:

```bash
py -3.10 -m venv [Desired_Folder_Path]
```

> Example:
```bash
py -3.10 -m venv .venv
```

### 3) Activate the virtual environment

Windows (cmd):
```bash
./Scripts/activate
```
Use the activation command that matches the folder name you chose (`.venv` vs `venv`) and your shell.

### 4) Install dependencies
```bash
pip install -r requirements.txt
```

---

## MongoDB Setup (Local)

### 1) Install MongoDB tools
- Download **mongosh** and **MongoDB Database Tools**
- Add them to your **system PATH**

### 2) Start Mongo Shell
```bash
mongosh
```

### 3) Select your database
```javascript
use aac
```

> You can use any database name you wish (not required to be `aac`), but keep the names consistent with your environment variables and import command.

### 4) Create the admin user
Run the following (update values to match your chosen DB name):

```javascript
db.createUser(
  {
    user: "yourAdminUsername",
    pwd: "yourStrongPassword",
    roles: [
      { role: "dbAdmin", db: "yourDatabaseName" },
      { role: "readWrite", db: "yourDatabaseName" }
    ]
  }
)
```

### 5) Test authentication
Exit `mongosh`, then run:

```bash
mongosh -u "aacuser" -p --authenticationDatabase "aac"
```

> Replace `aacuser` / `aac` with the username and DB you created if different.

---

## Import the Dataset

From the repository root (or adjust the CSV path accordingly):

```bash
mongoimport --username="YourUsername" --port=27017 --host="127.0.0.1" --db YourDBName --collection animals --authenticationDatabase YourDBName --drop --type csv --headerline --file ./aac_shelter_outcomes.csv
```

After this step, the `animals` collection should be populated and ready.

---

## Configure Environment Variables

Create a `.env` file in the repository root (recommended), or set environment variables in your shell.

Example `.env`:

```bash
MONGO_USER=yourAdminUsername
MONGO_PASS=yourStrongPassword
MONGO_HOST=127.0.0.1
MONGO_PORT=27017
MONGO_DB=aac
MONGO_COL=animals
MONGO_AUTHSOURCE=aac
```

---

## Run the Dashboard

### 1) Start JupyterLab
```bash
jupyter lab
```

### 2) Open the notebook and run the cell
- Open `Dashboard.ipynb`
- Click the **Run/Play** button at the top to execute the notebook cell(s)

### 3) Open the dashboard link
At the bottom of the output, a URL will be printed. Open that address in your browser to view the dashboard.

---

## Notes and Troubleshooting

### Why use projection + pagination?
- **Projection** returns only the fields needed by the UI, reducing payload size.
- **Pagination** limits how many records are loaded at once, improving responsiveness and preventing the browser from freezing on large datasets.
- The table can still represent the **full dataset** via page navigation (server-side pagination), while only fetching the active page.

### Map not displaying?
- Ensure the selected record includes valid:
  - `location_lat`
  - `location_long`
- If the dataset contains missing coordinates, some rows will not render a marker.

---

## License
For educational/portfolio use.