# DWBI-Final-Project
DWBI Group 5 Final Project

Submitted by:  
Maria Michaela Dionson  
Joseph Michael Calunsag  
Mirko Jankovic


## Setup Instructions

### 1. Install Python Dependencies
```bash
pip install pandas sqlalchemy
```

### 2. Run the ETL Script
This will load all CSV data into a SQLite database (`data/electronics_dw.db`):
```bash
cd scripts
python etl.py
```

### 3. Open the Analysis Notebook
Open `notebooks/analysis.ipynb` in Jupyter or VS Code and run all cells.

## Project Structure
```
DWBI Finals/
├── data/                    # CSV files + SQLite database
├── notebooks/               # Jupyter notebooks for analysis
├── scripts/                 # ETL scripts
└── README.md
```
