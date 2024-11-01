OPEN F1

# Formula 1 Data Engineering and Analysis with Databricks

## Overview
This repository contains Databricks notebooks used for fetching, transforming, and analyzing Formula 1 race data. The project retrieves data from the OpenF1 API and processes it using PySpark in Databricks. The main focus of this project is to analyze stints and driver performance across Singapore race in 2023.

OR:
This project focuses on building robust data engineering pipelines for Formula 1 race data using Databricks. It covers the entire workflow from data fetching (via APIs), transformation, and loading into Delta tables, followed by analysis of key race metrics.

## Features
- Fetch data from OpenF1 API for various race datasets (e.g., drivers, stints, positions, sessions,…).
- Process data using PySpark in Databricks.
- Save results to Delta tables or CSV files for further analysis.
- Orchestrate workflows using reusable pipelines.

## Notebooks in this Repository
1. **fetching_logic.py**: Contains reusable functions to fetch datasets from the OpenF1 API.
2. **fetching_execution.py**: Executes the data fetching workflows for a given year and saves the datasets.
3. **reader_factory_openf1.py**: Factory class to read data from different formats (CSV, Delta tables).
4. **extractor_openf1.py**: Extracts data from saved tables or files into the pipeline for processing.
5. **transform_openf1.py**: Handles data transformations for race data, including specific logic for events like the Singapore GP.
6. **loader_factory_openf1.py**: Factory class to load transformed data into different formats (Delta tables, CSV files).
7. **loader_openf1.py**: Implements the logic for loading transformed data into final storage locations.
8. **analysis_openf1.py**: Orchestrates the entire workflow, from fetching data to loading the final results.


## Setup Instructions
1. **Clone the repository**:  
    ```bash
    git clone https://github.com/your-username/your-repo-name.git
    ```
2. **Install dependencies**:  
   It works with Databricks environments.
3. **Databricks Setup**:  
   - Upload the notebooks to your Databricks workspace.
   - Ensure a compatible Spark cluster is running.

## How to Run the Project
1. **Fetch Data**: Run the `fetching_execution.py` notebook to retrieve data from the OpenF1 API. This will save the fetched datasets (e.g., drivers, stints) into Delta tables or CSV files.
2. **Transform Data**: Process the saved data by running the ‘transform_openf1.py’ notebook. This will apply necessary transformations, such as filtering, aggregation, and cleaning of the raw data.
3. **Load Data**: Use the ‘loader_openf1.py’ notebook to load the transformed data into Delta tables or CSV files, ready for analysis or further processing.

## Results
Here are some insights from the race data:

￼
￼

￼



## License
This project is licensed under the MIT License - see the LICENSE file for details.
