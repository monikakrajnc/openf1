# Databricks notebook source


import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from IPython.display import display, HTML
from pyspark.sql.functions import to_timestamp, col, max, row_number, when
from pyspark.sql.window import Window


class Transformer:
    """
    Transforms the extracted data according to the business logic.
    """

    def __init__(self):
        pass

    def transform(self, inputDFs):
        """
        Abstract method for transforming data.
        Should be implemented by subclasses to define transformation logic.
        """
        pass


class SingaporeRaceTransformer(Transformer):
    """
    Transforms data related to the Singapore Grand Prix race.
    The transformations include extracting final positions, stint durations, 
    compound types, and other race-specific metrics for each driver.
    """

    def transform(self, inputDFs):
        """
        Transforms input DataFrames for the Singapore race, including data for 
        sessions, stints, positions, and drivers. Joins the datasets, calculates 
        stint durations, filters data for the Singapore race, and extracts final 
        positions and other relevant information.

        Args:
            inputDFs (dict): A dictionary containing extracted DataFrames for 
                             sessions, stints, positions, and drivers.
        
        Returns:
            pyspark.sql.DataFrame: A transformed DataFrame with race results 
                                   and driver information.
        """
        # Session data transformation
        sessionsInputDF = inputDFs.get("sessionsInputDF")
        print("sessionsInputDF in transform")
        sessionsInputDF.show(5)

        # Drop multiple columns from the DataFrame
        sessionsInputDF_dropped = sessionsInputDF.drop('circuit_key', 'country_code', 'country_key', 'year', 'gmt_offset')

        # Convert data types
        sessionsInputDF_converted = sessionsInputDF_dropped.withColumn("meeting_key", col("meeting_key").cast("int"))
        sessionsInputDF_converted = sessionsInputDF_converted.withColumn("session_key", col("session_key").cast("int"))
        sessionsInputDF_converted = sessionsInputDF_converted.withColumn("date_start", col("date_start").cast("timestamp"))
        sessionsInputDF_converted = sessionsInputDF_converted.withColumn("date_end", col("date_end").cast("timestamp"))

        print("sessionsInputDF transformed")
        sessionsInputDF_converted.show(5)


        # Stints data transformation
        stintsInputDF = inputDFs.get("stintsInputDF")
        print("stintsInputDF in transform")
        stintsInputDF.show(5)   

        # Converting data types
        stintsInputDF_converted = stintsInputDF.withColumn("driver_number", col("driver_number").cast("int"))
        stintsInputDF_converted = stintsInputDF_converted.withColumn("meeting_key", col("meeting_key").cast("int"))
        stintsInputDF_converted = stintsInputDF_converted.withColumn("lap_start", col("lap_start").cast("int"))
        stintsInputDF_converted = stintsInputDF_converted.withColumn("lap_end", col("lap_end").cast("int"))
        stintsInputDF_converted = stintsInputDF_converted.withColumn("session_key", col("session_key").cast("int"))
        stintsInputDF_converted = stintsInputDF_converted.withColumn("stint_number", col("stint_number").cast("int"))
        stintsInputDF_converted = stintsInputDF_converted.withColumn("tyre_age_at_start", col("tyre_age_at_start").cast("int")) 

        # Perform a join on the "session_key" column
        stints_sessions_joined = stintsInputDF_converted.join(sessionsInputDF_converted, on=['session_key', 'meeting_key'], how = "left")
        print("Joined stints with sessions after transformation")
        stints_sessions_joined.show(5)


        # Positions data transformation
        positionsInputDF = inputDFs.get("positionsInputDF")
        print("positionsInputDF in transform")
        positionsInputDF.show(5)

        # Converting data types
        positionsInputDF_converted = positionsInputDF.withColumn("driver_number", col("driver_number").cast("int"))
        positionsInputDF_converted = positionsInputDF_converted.withColumn("meeting_key", col("meeting_key").cast("int"))
        positionsInputDF_converted = positionsInputDF_converted.withColumn("position", col("position").cast("int"))
        positionsInputDF_converted = positionsInputDF_converted.withColumn("session_key", col("session_key").cast("int"))
        positionsInputDF_converted = positionsInputDF_converted.withColumn("date", col("date").cast("timestamp"))

        print("positionsInputDF transformed")
        positionsInputDF_converted.show(5)

        # Save the Spark DataFrame to a CSV file
        # position.write.csv("dbfs:/FileStore/tables/OpenF1/position_converted2023.csv", header=True, mode="overwrite")


        # Filter data to only Singapore race
        position_singapore_race = positionsInputDF_converted.filter((col("meeting_key") == 1219) & (col("session_key") == 9165))  

        # Group by "driver_number" and count the occurrences of "position"
        number_positions = position_singapore_race.groupBy("driver_number").agg({"position": "count"})

        # Rename the count column for clarity
        number_positions = number_positions.withColumnRenamed("count(position)", "number_of_positions")


        # Final position for each driver
        # Define the window specification
        window_spec = Window.partitionBy("driver_number").orderBy(col("date").desc())
        # Add a row number column based on the window specification
        df_with_rownum = position_singapore_race.withColumn("row_number", row_number().over(window_spec))
        # Filter the DataFrame to include only the last position for each driver
        last_position_df = df_with_rownum.filter(col("row_number") == 1).drop("row_number")

        # Show the result
        print("Final position for each driver")
        last_position_df.show()


        # Drivers data transformation
        driversInputDF = inputDFs.get("driversInputDF")
        print("driversInputDF in transform")
        driversInputDF.show(7)

        # Filter rows where "driver_number" is "40"
        lawson = driversInputDF.where(col("driver_number") == 40)

        # Drop rows with any None (NaN) and duplicate values
        driversInputDF_cleaned = driversInputDF.dropna()
        driversInputDF_cleaned = driversInputDF_cleaned.dropDuplicates(['broadcast_name'])

        # Drop unwanted columns
        driversInputDF_dropped = driversInputDF_cleaned.drop('meeting_key', 'session_key', 'team_colour')
        print("driversInputDF transformed")
        driversInputDF_dropped.show(20)

        # Perform a join on the "session_key" column
        stints_sessions_drivers_joined = stints_sessions_joined.join(driversInputDF_dropped, on=['driver_number'], how = 'left')
        print("stints_sessions_drivers_joined")
        stints_sessions_drivers_joined.show(10)

        # Check the schema (data types) 
        stints_sessions_drivers_joined.printSchema()
        # Save the Spark DataFrame to a CSV file
        # stints_sessions_drivers_converted.write.csv("dbfs:/FileStore/tables/OpenF1/stints_sessions_drivers2023.csv", header=True, mode="overwrite")


        # Enter info for driver number 40, Liam Lawson
        # Extract the first row of the DataFrame as a Row object
        first_row = lawson.first()
        # Extract values from the first row
        lawson_broadcast_name = first_row["broadcast_name"]
        lawson_team_name = first_row["team_name"]
        lawson_first_name = first_row["first_name"]
        lawson_last_name = first_row["last_name"]
        lawson_full_name = first_row["full_name"]
        lawson_country_code = first_row["country_code"]
        lawson_name_acronym = first_row["name_acronym"]

        # Update the target DataFrame "stints_sessions_drivers_joined"
        stints_sessions_drivers_joined = stints_sessions_drivers_joined.withColumn(
                        "broadcast_name",
                    when(stints_sessions_drivers_joined["driver_number"] == 40, lawson_broadcast_name).otherwise(stints_sessions_drivers_joined["broadcast_name"])
                    ).withColumn(
                        "team_name",
                    when(stints_sessions_drivers_joined["driver_number"] == 40, lawson_team_name).otherwise(stints_sessions_drivers_joined["team_name"])
                    ).withColumn(
                        "first_name",
                    when(stints_sessions_drivers_joined["driver_number"] == 40, lawson_first_name).otherwise(stints_sessions_drivers_joined["first_name"])
                    ).withColumn(
                        "last_name",
                    when(stints_sessions_drivers_joined["driver_number"] == 40, lawson_last_name).otherwise(stints_sessions_drivers_joined["last_name"])
                    ).withColumn(
                        "full_name",
                    when(stints_sessions_drivers_joined["driver_number"] == 40, lawson_full_name).otherwise(stints_sessions_drivers_joined["full_name"])
                    ).withColumn(
                        "name_acronym",
                    when(stints_sessions_drivers_joined["driver_number"] == 40, lawson_name_acronym).otherwise(stints_sessions_drivers_joined["name_acronym"])
                    ).withColumn(
                        "country_code",
                    when(stints_sessions_drivers_joined["driver_number"] == 40, lawson_country_code).otherwise(stints_sessions_drivers_joined["country_code"])
                    )


        # Filter to only Singapore race data
        # Filter rows where "circuit name" contains "Singapore"
        singapore = stints_sessions_drivers_joined.filter(col("circuit_short_name").contains("Singapore"))  

        # Filter rows to only contain "Race" data
        singapore_race = singapore.where(col("session_name") == "Race")  

        # Add a new column "stint_duration" which is the difference between "lap_end" and "lap_start"
        singapore_race = singapore_race.withColumn("stint_duration", col("lap_end") - col("lap_start"))
        # Save the Spark DataFrame to a CSV file
        # singapore_race.write.csv("dbfs:/FileStore/tables/OpenF1/singapore_race2023.csv", header=True, mode="overwrite")

        print("Singapore race")
        singapore_race.show(5)


        # Finding the last stint for each driver
        # Define the window specification
        window_spec = Window.partitionBy("driver_number").orderBy(col("stint_number").desc())
        # Add a row number column based on the window specification
        df_with_rownum = singapore_race.withColumn("row_number", row_number().over(window_spec))
        # Filter the DataFrame to include only the last stint for each driver
        last_stint_df = df_with_rownum.filter(col("row_number") == 1).drop("row_number")

        # Show the result
        print("Last stint for each driver")
        last_stint_df.show()

        # Perform a join on the "session_key" column
        singapore_race_final_position = last_stint_df.join(last_position_df, on=["session_key", "driver_number", "meeting_key"], how = "left")

        # Show the result
        print("Singapore race final position")
        singapore_race_final_position.show()
        # Save the Spark DataFrame to a CSV file
        #singapore_race_final_position.write.csv("dbfs:/FileStore/tables/OpenF1/singapore_race_final_position2023.csv", header=True, mode="overwrite")


        # Final result
        # Combine race final position with all the positions throught the race
        result_spark = singapore_race_final_position.join(number_positions, on=["driver_number"], how = "left")
        # Sort the DataFrame in ascending order based on the "position" column
        result_spark_sorted = result_spark.orderBy("position")

        # Show final result
        print("Singapore race results")
        result_spark_sorted.show()
        # Save the Spark DataFrame to a CSV file
        #sorted_result_spark.write.csv("dbfs:/FileStore/tables/OpenF1/singapore_race_final_position_with_number_of_positions2023.csv", header=True, mode="overwrite")

        # Create a smaller result
        # Create a "smaller" DataFrame by selecting specific columns
        result_smaller = result_spark_sorted.select(
                    "position", 
                    "full_name", 
                    "driver_number", 
                    "team_name", 
                    "stint_number", 
                    "stint_duration", 
                    "compound", 
                #    "circuit_short_name",
                    "number_of_positions"
                )
        
        print("Singapore race final drivers position, compound, last stint duration, number of positions changes")
        result_smaller.show()

        # Convert to Pandas DataFrame
        pandas_df = result_smaller.toPandas()

        # Display all rows as an HTML table
        html_output = pandas_df.to_html(index=False)
        display(HTML(html_output))

        # Save the Spark DataFrame to a CSV file
        #result_smaller.write.csv("dbfs:/FileStore/tables/OpenF1/singapore_race_final_position_with_number_of_positions_small2023.csv", header=True, mode="overwrite")

        return result_smaller
