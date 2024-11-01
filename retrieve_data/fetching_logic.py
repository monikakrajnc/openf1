# Databricks notebook source


import pandas as pd
import requests
from io import StringIO
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("FetchingOpenF1").getOrCreate()


class DatasetFetcher:
    """
    A class to fetch and save datasets from the OpenF1 API.
    
    Attributes:
        dataset_name (str): The name of the dataset to fetch (e.g., 'drivers', 'stints').
        columns (list): List of columns expected in the dataset.
        base_url (str): The base URL for the OpenF1 API.
        dataframe (pd.DataFrame): A Pandas DataFrame to store fetched data.
    """
    
    def __init__(self, dataset_name, columns, base_url="https://api.openf1.org/v1"):
        """Initializes the DatasetFetcher with the dataset name, columns, and base URL."""
        self.dataset_name = dataset_name
        self.columns = columns
        self.base_url = base_url
        self.dataframe = pd.DataFrame(columns=self.columns)

    def fetch_data(self, session_keys):
        """
        Fetches data for multiple session keys from the OpenF1 API.
        
        Args:
            session_keys (list): List of session keys to fetch data for.
        """
        for session_key in session_keys:
            try:
                print(f"Fetching data for session: {session_key} for dataset: {self.dataset_name}")
                # Send a GET request to fetch the data as CSV
                response = requests.get(f"{self.base_url}/{self.dataset_name}?session_key={session_key}&csv=true")

                if response.status_code == 200:
                    # Convert the CSV response to a DataFrame and append to the main dataframe
                    csv_data = response.text
                    csv_file = StringIO(csv_data)
                    df = pd.read_csv(csv_file)
                    self.dataframe = pd.concat([self.dataframe, df], ignore_index=True)
                    print(f"Fetched data for session: {session_key}. Total rows: {len(self.dataframe)}")
                else:
                    print(f"Failed to fetch data for session: {session_key}. Status code: {response.status_code}")
            except Exception as e:
                # Catch any errors that occur during the request
                print(f"Error fetching data for session: {session_key}. Error: {str(e)}")

    def save_as_table(self, table_name):
        """
        Saves the fetched data as a Delta table in Spark.
        
        Args:
            table_name (str): The name of the Delta table to save data to.
        """
        if not self.dataframe.empty:
            # Convert Pandas DataFrame to Spark DataFrame and save as Delta table
            spark_df = spark.createDataFrame(self.dataframe)
            spark_df.write.format("delta").mode("overwrite").saveAsTable(table_name)
            print(f"Data saved as Delta table: {table_name}")
        else:
            print("No data to save as Delta table.")

    def save_to_csv(self, file_name):
        """
        Saves the fetched data as a CSV file in DBFS.
        
        Args:
            file_name (str): The name of the CSV file to save data to.
        """
        if not self.dataframe.empty:
            try:
                # Check if directory exists, create if necessary
                if not dbutils.fs.ls("dbfs:/FileStore/tables/OpenF1"):
                    dbutils.fs.mkdirs("/FileStore/tables/OpenF1")
                # Save CSV data
                csv_data = self.dataframe.to_csv(index=False)
                dbutils.fs.put(f"FileStore/tables/OpenF1/{file_name}_local.csv", csv_data, overwrite=True)

                # Also save as a Spark DataFrame for parallel processing
                spark_df = spark.createDataFrame(self.dataframe)
                spark_df.write.csv(f"dbfs:/FileStore/tables/OpenF1/{file_name}.csv", header=True)
                print(f"Data saved as CSV: {file_name}.csv")
            except Exception as e:
                # Catch any errors during the saving process
                print(f"Error saving CSV: {str(e)}")
        else:
            print("No data to save as CSV.")

    @staticmethod
    def fetch_sessions_for_year(year):
        """
        Fetches all session keys for a given year from the OpenF1 API.
        
        Args:
            year (int): The year to fetch session keys for.
        
        Returns:
            list: A list of session keys for the specified year.
        """
        url = f"https://api.openf1.org/v1/sessions?year={year}&csv=true"
        response = requests.get(url)

        if response.status_code == 200:
            # Parse the CSV response into a DataFrame and extract session keys
            csv_data = response.text
            csv_file = StringIO(csv_data)
            sessions_df = pd.read_csv(csv_file)
            # Extract session_keys and return as a list
            session_keys = sessions_df["session_key"].unique().tolist()
            print(f"Found {len(session_keys)} sessions for the year {year}")
            return session_keys
        else:
            # Handle unsuccessful responses
            print(f"Failed to fetch sessions for year {year}. Status code: {response.status_code}")
            return []
