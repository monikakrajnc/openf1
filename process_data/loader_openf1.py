# Databricks notebook source


from loader_factory_openf1 import *

# Alternative, only in Databricks
# %run "./loader_factory_openf1"


class AbstractLoader:
    """
    Base class for loading transformed data into the appropriate storage location.
    """

    def __init__(self, transformedDF):
        """
        Initializes the loader with the transformed DataFrame.
        
        Args:
            transformedDF (DataFrame): The Spark DataFrame that has been transformed and is ready to be loaded.
        """
        self.transformedDF = transformedDF

    def sink(self):
        """
        Abstract method. This method should be overridden by subclasses to specify where and how to load the data.
        """
        pass


class SingaporeRaceLoader(AbstractLoader):
    """
    Loads the data of Singapore race results, including the final positions, last used compound, stint duration, 
    and the number of position changes during the race.
    """

    def sink(self):
        """
        Loads the Singapore race data into DBFS.
        """
        get_sink_source(
            sink_type = "dbfs",
            df = self.transformedDF, 
            path = "dbfs:/FileStore/tables/OpenF1/output/SingaporeRaceFinalPositions", 
            method = "overwrite"
        ).load_data_frame()
