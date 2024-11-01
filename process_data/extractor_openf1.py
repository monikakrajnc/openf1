# Databricks notebook source


from reader_factory_openf1 import *

# Alternative, only in Databricks
# %run "./reader_factory_openf1"


class Extractor:
    """
    Abstract class for data extraction.
    """
    
    def __init__(self):
        pass

    def extract(self):
        """
        Abstract method for extracting data.
        This method should be implemented by subclasses to define how data is extracted.
        """
        pass


class Formula1Extractor(Extractor):
    """
    Extracts Formula 1 data from CSV and Delta sources.
    """

    def extract(self):
        """ 
        Extracts sessions, stints, positions data from CSV files and driver data from a Delta table.

        Returns:
            dict: A dictionary containing the extracted DataFrames for sessions, stints, positions, and drivers.
        """
        sessionsInputDF = get_data_source(data_type = "csv", file_path = "dbfs:/FileStore/tables/OpenF1/sessions2023.csv").get_data_frame()    

        stintsInputDF = get_data_source(data_type = "csv", file_path = "dbfs:/FileStore/tables/OpenF1/stints2023.csv").get_data_frame()

        positionsInputDF = get_data_source(data_type = "csv", file_path = "dbfs:/FileStore/tables/OpenF1/position2023.csv").get_data_frame()

        driversInputDF = get_data_source(data_type = "delta", file_path = "default.drivers2023_delta_table").get_data_frame()

        inputDFs = {"sessionsInputDF": sessionsInputDF, 
                    "stintsInputDF": stintsInputDF,
                    "positionsInputDF": positionsInputDF, 
                    "driversInputDF": driversInputDF}
        
        return inputDFs
