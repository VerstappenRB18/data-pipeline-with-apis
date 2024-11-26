import os
import time
import json
import fastf1
import pandas as pd
from kafka import KafkaProducer


KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL")
TOPIC_NAME = os.environ.get("TOPIC_NAME")
SLEEP_TIME = int(os.environ.get("SLEEP_TIME", 1))

# Load FastF1 session
session = fastf1.get_session(2023, "Austria", "R")
session.load()

target_drivers = ["VER", "LEC"]  # Driver abbreviations for Max Verstappen and Charles Leclerc

def format_lap_time(td):
    """Convert lap time to MM:SS.mmm format."""
    if pd.isna(td):  # Handle missing lap times
        return None
    total_seconds = td.total_seconds()
    minutes, seconds = divmod(total_seconds, 60)
    return f"{int(minutes):02}:{seconds:06.3f}"  # Format to MM:SS.mmm

def format_sector_time(td):
    """Convert a timedelta to SS.mmm format."""
    if pd.isna(td): # Handle missing sector times
        return None
    total_seconds = td.total_seconds()
    return f"{total_seconds:06.3f}"  # Format to SS.mmm

# Helper function to generate lap data
def get_lap_data(driver, lap):
    return {
        "session_name": str(session.name),
        "track": str(session.event['EventName']),
        "date": session.date.isoformat(),
        "driver": str(driver),
        "driver_name": str(session.get_driver(driver)['FullName']),
        "team": str(session.get_driver(driver)['TeamName']),
        "lap_number": int(lap['LapNumber']),
        "lap_time": format_lap_time(lap['LapTime']),
        "position": int(lap['Position']) if not pd.isna(lap['Position']) else None,
        "tyre_compound": str(lap['Compound']),
        "tyre_life": int(lap['TyreLife']) if not pd.isna(lap['TyreLife']) else None,
        "pit_stop": bool(not pd.isna(lap['PitInTime'])),
        "stint": int(lap['Stint']) if not pd.isna(lap['Stint']) else None,
        "fresh_tyre": bool(lap['FreshTyre']) if not pd.isna(lap['FreshTyre']) else None,
        "sector1_time": format_sector_time(lap['Sector1Time']),
        "sector2_time": format_sector_time(lap['Sector2Time']),
        "sector3_time": format_sector_time(lap['Sector3Time']),
    }

def run():
    iterator = 0
    print(f"Setting up FastF1 producer at {KAFKA_BROKER_URL}")
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER_URL],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    )

    # Loop through the target drivers
    for driver in target_drivers:
        driver_laps = session.laps.pick_drivers(driver)
        
        for _, lap in driver_laps.iterrows():
            lap_data = get_lap_data(driver, lap)

            # Send lap data to Kafka
            print(f"Sending lap data iteration {iterator} for driver {driver}")
            producer.send(TOPIC_NAME, value=lap_data)
            print("Lap data sent")
            time.sleep(SLEEP_TIME)  # Wait between sending records
            iterator += 1
    producer.close()


if __name__ == "__main__":
    run()
