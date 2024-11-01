# Databricks notebook source


# Import the DatasetFetcher class from the fetching logic notebook
from fetching_logic import *

# Alternative, only in Databricks
# %run "./fetching_logic"


def fetch_specific_datasets_for_year(datasets, year):
    """
    Fetches datasets for a specified year from the OpenF1 API.

    Args:
        datasets (list): List of dataset names to fetch.
        year (int): Year for which the datasets should be fetched.
    """
    # Fetch sessions for the specified year to get session keys
    session_keys = DatasetFetcher.fetch_sessions_for_year(year)

    if not session_keys:
        print(f"No session keys found for the year {year}. Aborting fetch.")
        return

    # Fetch specific datasets using the retrieved session keys
    for dataset in datasets:

        if dataset == "drivers":
            drivers_fetcher = DatasetFetcher(
                dataset_name="drivers",
                columns=["broadcast_name", "country_code", "driver_number", "first_name", "full_name", 
                         "headshot_url", "last_name", "meeting_key", "name_acronym", 
                         "session_key", "team_colour", "team_name"]
            )
            drivers_fetcher.fetch_data(session_keys)
            drivers_fetcher.save_as_table(f"drivers{year}_delta_table")
            drivers_fetcher.save_to_csv(f"drivers{year}")

        elif dataset == "intervals":
            intervals_fetcher = DatasetFetcher(
                dataset_name="intervals",
                columns=["date", "driver_number", "gap_to_leader", "interval", "meeting_key", "session_key"]
            )
            intervals_fetcher.fetch_data(session_keys)
            intervals_fetcher.save_as_table(f"intervals{year}_delta_table")
            intervals_fetcher.save_to_csv(f"intervals{year}")

        elif dataset == "stints":
            stints_fetcher = DatasetFetcher(
                dataset_name="stints",
                columns=["compound", "driver_number", "lap_end", "lap_start", 
                         "meeting_key", "session_key", "stint_number", "tyre_age_at_start"]
            )
            stints_fetcher.fetch_data(session_keys)
            stints_fetcher.save_as_table(f"stints{year}_delta_table")
            stints_fetcher.save_to_csv(f"stints{year}")

        elif dataset == "laps":
            laps_fetcher = DatasetFetcher(
                dataset_name="laps",
                columns=["date_start", "driver_number", "duration_sector_1", "duration_sector_2", 
                         "duration_sector_3", "i1_speed", "i2_speed", "is_pit_out_lap", "lap_duration", "lap_number", "meeting_key", "segments_sector_1", "segments_sector_2", "segments_sector_3", "session_key", "st_speed"]
            )
            laps_fetcher.fetch_data(session_keys)
            laps_fetcher.save_as_table(f"laps{year}_delta_table")
            laps_fetcher.save_to_csv(f"laps{year}")

        elif dataset == "location":
            location_fetcher = DatasetFetcher(
                dataset_name="location",
                columns=["date", "driver_number", "meeting_key", "session_key", "x", "y", "z"]
            )
            location_fetcher.fetch_data(session_keys)
            location_fetcher.save_as_table(f"location{year}_delta_table")
            location_fetcher.save_to_csv(f"location{year}")

        elif dataset == "meetings":
            meetings_fetcher = DatasetFetcher(
                dataset_name="meetings",
                columns=["broadcast_name", "country_code", "driver_number", "first_name", "full_name", 
                         "headshot_url", "last_name", "meeting_key", "name_acronym", 
                         "session_key", "team_colour", "team_name"]
            )
            meetings_fetcher.fetch_data(session_keys)
            meetings_fetcher.save_as_table(f"meetings{year}_delta_table")
            meetings_fetcher.save_to_csv(f"meetings{year}")

        elif dataset == "pit":
            pit_fetcher = DatasetFetcher(
                dataset_name="pit",
                columns=["date", "driver_number", "lap_number", "meeting_key", "pit_duration", "session_key"]
            )
            pit_fetcher.fetch_data(session_keys)
            pit_fetcher.save_as_table(f"pit{year}_delta_table")
            pit_fetcher.save_to_csv(f"pit{year}")

        elif dataset == "position":
            position_fetcher = DatasetFetcher(
                dataset_name="position",
                columns=["date", "driver_number", "meeting_key", "position", "session_key"]
            )
            position_fetcher.fetch_data(session_keys)
            position_fetcher.save_as_table(f"position{year}_delta_table")
            position_fetcher.save_to_csv(f"position{year}")

        elif dataset == "race_control":
            race_control_fetcher = DatasetFetcher(
                dataset_name="race_control",
                columns=["category", "date", "driver_number", "flag", "lap_number", "meeting_key", "message",      
                         "scope", "sector", "session_key"]
            )
            race_control_fetcher.fetch_data(session_keys)
            race_control_fetcher.save_as_table(f"race_control{year}_delta_table")
            race_control_fetcher.save_to_csv(f"race_control{year}")

        elif dataset == "sessions":
            sessions_fetcher = DatasetFetcher(
                dataset_name="sessions",
                columns=["circuit_key", "circuit_short_name", "country_code", "country_key",
                         "country_name", "date_end", "date_start", "gmt_offset", "location",
                         "meeting_key", "session_key", "session_name", "session_type", "year"]
            )
            sessions_fetcher.fetch_data(session_keys)
            sessions_fetcher.save_as_table(f"sessions{year}_delta_table")
            sessions_fetcher.save_to_csv(f"sessions{year}")

        elif dataset == "car_data":
            car_data_fetcher = DatasetFetcher(
                dataset_name="car_data",
                columns=["broadcast_name", "country_code", "driver_number", "first_name", "full_name", 
                         "headshot_url", "last_name", "meeting_key", "name_acronym", 
                         "session_key", "team_colour", "team_name"]
            )
            car_data_fetcher.fetch_data(session_keys)
            car_data_fetcher.save_as_table(f"car_data{year}_delta_table")
            car_data_fetcher.save_to_csv(f"car_data{year}")

        elif dataset == "team_radio":
            team_radio_fetcher = DatasetFetcher(
                dataset_name="team_radio",
                columns=["broadcast_name", "country_code", "driver_number", "first_name", "full_name", 
                         "headshot_url", "last_name", "meeting_key", "name_acronym", 
                         "session_key", "team_colour", "team_name"]
            )
            team_radio_fetcher.fetch_data(session_keys)
            team_radio_fetcher.save_as_table(f"team_radio{year}_delta_table")
            team_radio_fetcher.save_to_csv(f"team_radio{year}")

        elif dataset == "weather":
            drivers_fetcher = DatasetFetcher(
                dataset_name="weather",
                columns=["air_temperature", "date", "humidity", "meeting_key", "pressure", "rainfall", 
                         "session_key", "track_temperature", "wind_direction", "wind_speed"]
            )
            weather_fetcher.fetch_data(session_keys)
            weather_fetcher.save_as_table(f"weather{year}_delta_table")
            weather_fetcher.save_to_csv(f"weather{year}")
        
        else:
            print(f"Dataset '{dataset}' is not recognized.")

# COMMAND ----------

# Example usage of fetch_specific_datasets_for_year function
datasets_to_fetch = ["drivers", "stints"]  # Specify the datasets you want to fetch
selected_year = 2023  # Replace with the desired year
fetch_specific_datasets_for_year(datasets_to_fetch, selected_year)
