# Flight ETL Pipeline

## Project Overview
This project demonstrates an end-to-end **ETL pipeline** for flight data.  
It covers the full lifecycle of data engineering: extraction, transformation, loading, and analysis.  
The pipeline automates the processing of raw flight information into clean, structured, and analytical datasets.

## Architecture
1. **Extract:**  
   Flight data is fetched and stored as raw Parquet files. The extract step handles file naming, date stamps, and basic validation.

2. **Transform:**  
   Raw data is cleaned and transformed using Python and PySpark.  
   Key transformations include:
   - Parsing date and time columns  
   - Handling null and invalid values  
   - Deriving delay metrics (arrival, departure, total delay)  
   - Standardizing carrier codes and airport identifiers  
   - Saving processed data as partitioned Parquet files

3. **Load:**  
   The transformed data is loaded into the destination layer (Azure Data Lake or local storage).  
   The process supports both manual and automated uploads via helper scripts.

4. **Analysis:**  
   The processed dataset is analyzed using Pandas and Matplotlib.  
   Example insights include:
   - Average delay by airline and airport  
   - Daily and monthly flight trends  
   - Cancellation and delay distributions  
   - On-time performance visualization

## Directory Structure
flight-pipeline/
│
├── src/
│ ├── extract.py
│ ├── transform.py
│ ├── load.py
│ └── utils/
│ └── io.py
│
├── data/
│ ├── raw/
│ ├── processed/
│ └── analysis/
│
├── analysis/
│ ├── flight_analysis.ipynb
│ └── visuals/
│
└── README.md

## Key Features
- Modular ETL scripts with configurable paths and parameters  
- Automatic timestamp-based file naming for version control  
- Clean schema generation and Parquet-based data management  
- Optional cloud integration with Azure Data Lake  
- Reproducible analysis notebooks for visualization and reporting  

## Example Analysis
The notebook `analysis/flight_analysis.ipynb` explores:
- Delay patterns per airline  
- Flight volume trends by month and route  
- Top delayed airports  
- Overall cancellation rates  
