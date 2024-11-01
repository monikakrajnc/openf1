# Databricks notebook source


class DataSink:
    """
    Abstract class representing a generic data sink where data will be loaded. 
    """
    
    def __init__(self, df, path, method, params):
        """
        Initializes the DataSink with a DataFrame, path, method, and optional parameters.
        
        Args:
            df (DataFrame): The Spark DataFrame to be saved.
            path (str): The destination path for saving the data.
            method (str): The saving method (e.g., "overwrite", "append").
            params (dict): Additional parameters, like partitioning options (if applicable).
        """
        self.df = df
        self.path = path
        self.method = method
        self.params = params 

    def load_data_frame(self):
        """
        Abstract method. This should be implemented in subclasses.
        """
        raise ValueError("Not implemented")


class LoadToDBFS(DataSink):
    """
    Loads data into DBFS without partitioning.
    """

    def load_data_frame(self):
        """
        Loads the DataFrame into DBFS (Databricks File System) without partitioning.
        """        
        self.df.write.mode(self.method).save(self.path)


class LoadToDBFSWithPartition(DataSink):
    """
    Loads data into DBFS with partitioning.
    """

    def load_data_frame(self):
        """
        Loads the DataFrame into DBFS with partitioning by specified columns.
        """        
        partitionByColumns = self.params.get("partitionByColumns")
        self.df.write.mode(self.method).partitionBy(*partitionByColumns).save(self.path)


class LoadToDeltaTable(DataSink):
    """
    Loads data as a Delta table.
    """
    
    def load_data_frame(self):  
        """
        Loads the DataFrame as a Delta table in Databricks.
        """             
        self.df.write.format("delta").mode(self.method).saveAsTable(self.path)


def get_sink_source(sink_type, df, path, method, params = None):
    """
    Returns the appropriate DataSink class based on the sink type (e.g., "dbfs", "delta").
    
    Args:
        sink_type (str): The type of sink to load data into (e.g., "dbfs", "delta").
        df (DataFrame): The Spark DataFrame to be saved.
        path (str): The destination path for saving the data.
        method (str): The saving method (e.g., "overwrite", "append").
        params (dict, optional): Additional parameters, like partitioning options.

    Returns:
        DataSink: An instance of a subclass of DataSink that handles loading data to the specified sink type.
    """
    if sink_type == "dbfs":
        return LoadToDBFS(df, path, method, params)
    elif sink_type == "dbfs_with_partition":
        return LoadToDBFSWithPartition(df, path, method, params)
    elif sink_type == "delta":
        return LoadToDeltaTable(df, path, method, params)
    else:
        return ValueError(f"Not implemented for sink type: {sink_type}")
