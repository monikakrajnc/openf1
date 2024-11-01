# Databricks notebook source


class DataSource:
    """
    Abstract class for reading data from various formats.
    """

    def __init__(self, path):
        """
        Initializes the DataSource with the given path.
        
        Args:
            path (str): The path to the data source (file or table name).
        """
        self.path = path

    def get_data_frame(self):
        """
        Abstract method to be implemented by subclasses for reading data.
        """
        raise ValueError("Not Implemented")


class CSVDataSource(DataSource):
    """
    Reads data from CSV files.
    """

    def get_data_frame(self):
        """
        Reads data from a CSV file and returns it as a Spark DataFrame.
        """
        return (spark.read.format("csv").option("header", "true").load(self.path))


class ParquetDataSource(DataSource):
    """
    Reads data from Parquet files.
    """

    def get_data_frame(self):
        """
        Reads data from a Parquet file and returns it as a Spark DataFrame.
        """
        return (spark.read.format("parquet").load(self.path))


class DeltaDataSource(DataSource):
    """
    Reads data from Delta tables.
    """

    def get_data_frame(self):
        """
        Reads data from a Delta table and returns it as a Spark DataFrame.
        """
        table_name = self.path
        return (spark.read.table(table_name))
    

def get_data_source(data_type, file_path):
    """
    Returns an appropriate DataSource object based on the data type.

    Args:
        data_type (str): The type of data source (csv, parquet, or delta).
        file_path (str): The file path or table name.

    Returns:
        DataSource: An instance of CSVDataSource, ParquetDataSource, or DeltaDataSource.
    
    Raises:
        ValueError: If the data_type is not implemented.
    """
    if data_type == "csv":
        return CSVDataSource(file_path)   
    elif data_type == "parquet":
        return ParquetDataSource(file_path)      
    elif data_type == "delta":
        return DeltaDataSource(file_path)    
    else:
        raise ValueError(f"Not implemented for data_type: {data_type}")
