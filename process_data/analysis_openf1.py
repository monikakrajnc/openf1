# Databricks notebook source


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("OpenF1stints2023.me").getOrCreate()

from reader_factory_openf1 import *
from extractor_openf1 import *
from transform_openf1 import *
from loader_openf1 import *

# Alternative, only in Databricks
# %run "./reader_factory_openf1"
# %run "./extractor_openf1"
# %run "./transform_openf1"
# %run "./loader_openf1"


class FirstWorkFlow:
    """
    ETL pipeline to generate the data for Singapore race result, including:
    - The last used compound,
    - Last stint duration,
    - Position changes during the race for each driver.
    """ 

    def __init__(self):
        pass

    def runner(self):
        """
        Executes the ETL workflow: Extract, Transform, and Load data for the Singapore race.
        """
        # Extract all required data from different source
        inputDFs = Formula1Extractor().extract()

        # Implement the Transformation logic
        firstTransformedDF = SingaporeRaceTransformer().transform(inputDFs)
        
        # Load all required data to differnt sink
        SingaporeRaceLoader(firstTransformedDF).sink()


class WorkFlowRunner:
    """
    Runs the specified workflow based on the provided name.
    """

    def __init__(self, name):
        """
        Initializes the WorkFlowRunner with a workflow name.
        
        Args:
            name (str): The name of the workflow to run.
        """
        self.name = name

    def runner(self):
        """
        Executes the workflow based on the name.
        """
        if self.name == "firstWorkFlow":
            return FirstWorkFlow().runner()
        else:
            raise ValueError(f"Not implemented for {self.name}")


name = "firstWorkFlow" 
workFlowrunner = WorkFlowRunner(name).runner()
