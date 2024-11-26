import os
import sys

import pandas as pd
from cassandra.cluster import BatchStatement, Cluster, ConsistencyLevel
from cassandra.query import dict_factory

tablename = os.getenv("weather.table", "weatherreport")
fakertable = os.getenv("fakertable.table", "fakerdata")
fastf1table = os.getenv("fastf1.table", "lapsummary")


CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST") if os.environ.get("CASSANDRA_HOST") else 'localhost'
CASSANDRA_KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE") if os.environ.get("CASSANDRA_KEYSPACE") else 'kafkapipeline'

WEATHER_TABLE = os.environ.get("WEATHER_TABLE") if os.environ.get("WEATHER_TABLE") else 'weather'
FAKER_TABLE = os.environ.get("FAKER_TABLE") if os.environ.get("FAKER_TABLE") else 'faker'
FASTF1_TABLE = os.environ.get("FASTF1_TABLE") if os.environ.get("FASTF1_TABLE") else 'fastf1'
    
    
def saveFakerDf(dfrecords):
    if isinstance(CASSANDRA_HOST, list):
        cluster = Cluster(CASSANDRA_HOST)
    else:
        cluster = Cluster([CASSANDRA_HOST])

    session = cluster.connect(CASSANDRA_KEYSPACE)

    counter = 0
    totalcount = 0

    cqlsentence = "INSERT INTO " + fakertable + " (name, building_number, street_name, city, country, postcode, year, job, company, company_email, ssn, passport_number, credit_card_provider, credit_card_number, credit_card_expire, credit_card_security_code) \
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    insert = session.prepare(cqlsentence)
    batches = []
    for idx, val in dfrecords.iterrows():
        batch.add(insert, (val['name'],
            val['building_number'],
            val['street_name'],
            val['city'],
            val['country'],
            val['postcode'],
            val['year'],
            val['job'],
            val['company'],
            val['company_email'],
            val['ssn'],
            val['passport_number'],
            val['credit_card_provider'],
            val['credit_card_number'],
            val['credit_card_expire'],
            val['credit_card_security_code']))
        counter += 1
        if counter >= 100:
            print('inserting ' + str(counter) + ' records')
            totalcount += counter
            counter = 0
            batches.append(batch)
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    if counter != 0:
        batches.append(batch)
        totalcount += counter
    rs = [session.execute(b, trace=True) for b in batches]

    print('Inserted ' + str(totalcount) + ' rows in total')

def saveWeatherreport(dfrecords):
    if isinstance(CASSANDRA_HOST, list):
        cluster = Cluster(CASSANDRA_HOST)
    else:
        cluster = Cluster([CASSANDRA_HOST])

    session = cluster.connect(CASSANDRA_KEYSPACE)

    counter = 0
    totalcount = 0

    cqlsentence = "INSERT INTO " + tablename + " (forecastdate, location, description, temp, feels_like, temp_min, temp_max, pressure, humidity, wind, sunrise, sunset) \
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    insert = session.prepare(cqlsentence)
    batches = []
    for idx, val in dfrecords.iterrows():
        batch.add(insert, (val['report_time'], val['location'], val['description'],
                           val['temp'], val['feels_like'], val['temp_min'], val['temp_max'],
                           val['pressure'], val['humidity'], val['wind'], val['sunrise'], val['sunset']))
        counter += 1
        if counter >= 100:
            print('inserting ' + str(counter) + ' records')
            totalcount += counter
            counter = 0
            batches.append(batch)
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    if counter != 0:
        batches.append(batch)
        totalcount += counter
    rs = [session.execute(b, trace=True) for b in batches]

    print('Inserted ' + str(totalcount) + ' rows in total')

def saveFastF1Df(dfrecords):
    # Set up the Cassandra cluster and session
    if isinstance(CASSANDRA_HOST, list):
        cluster = Cluster(CASSANDRA_HOST)
    else:
        cluster = Cluster([CASSANDRA_HOST])

    session = cluster.connect(CASSANDRA_KEYSPACE)

    counter = 0
    totalcount = 0

    # Adjusted CQL statement to match FastF1 lap data fields
    cqlsentence = "INSERT INTO " + fastf1table + " (session_name, track, date, driver, driver_name, team, lap_number, lap_time, position, tyre_compound, tyre_life, pit_stop, stint, fresh_tyre, sector1_time,sector2_time, sector3_time) \
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    insert = session.prepare(cqlsentence)
    batches = []

    # Iterate over the DataFrame and add records to the batch
    for idx, val in dfrecords.iterrows():
        batch.add(insert, (
            val['session_name'],
            val['track'],
            val['date'],
            val['driver'],
            val['driver_name'],
            val['team'],
            val['lap_number'],
            val['lap_time'],
            val['position'],
            val['tyre_compound'],
            val['tyre_life'],
            val['pit_stop'],
            val['stint'],
            val['fresh_tyre'],
            val['sector1_time'],
            val['sector2_time'],
            val['sector3_time']
        ))
        counter += 1
        if counter >= 100:  # Insert batch when it reaches 100 records
            print(f"Inserting {counter} records")
            totalcount += counter
            counter = 0
            batches.append(batch)
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    if counter != 0:  # Insert any remaining records
        batches.append(batch)
        totalcount += counter

    # Execute all batches
    rs = [session.execute(b, trace=True) for b in batches]

    print(f"Inserted {totalcount} rows in total")


def loadDF(targetfile, target):
    if target == 'weather':
        colsnames = ['description', 'temp', 'feels_like', 'temp_min', 'temp_max',
                     'pressure', 'humidity', 'wind', 'sunrise', 'sunset', 'location', 'report_time']
        dfData = pd.read_csv(targetfile, header=None,
                             parse_dates=True, names=colsnames)
        dfData['report_time'] = pd.to_datetime(dfData['report_time'])
        saveWeatherreport(dfData)
    elif target == 'faker':
        colsnames = ['name', 'building_number', 'street_name', 'city', 'country', 'postcode', 'year', 'job', 'company', 'company_email', 'ssn', 'passport_number', 'credit_card_provider', 'credit_card_number', 'credit_card_expire', 'credit_card_security_code']
        dfData = pd.read_csv(targetfile, header=None,
                             parse_dates=True, names=colsnames)
        saveFakerDf(dfData)
    elif target == 'fastf1':
        colsnames = ['session_name', 'track', 'date', 'driver', 'driver_name', 'team', 'lap_number', 'lap_time', 'position', 'tyre_compound', 'tyre_life', 'pit_stop', 'stint', 'fresh_tyre', 'sector1_time', 'sector2_time', 'sector3_time']
        dfData = pd.read_csv(targetfile, header=None,
                             parse_dates=True, names=colsnames)
        saveFastF1Df(dfData)


def getWeatherDF():
    return getDF(WEATHER_TABLE)
def getFakerDF():
    print(FAKER_TABLE)
    return getDF(FAKER_TABLE)
def getFastF1DF():
    print(FASTF1_TABLE)
    return getDF(FASTF1_TABLE)

def getDF(source_table):
    if isinstance(CASSANDRA_HOST, list):
        cluster = Cluster(CASSANDRA_HOST)
    else:
        cluster = Cluster([CASSANDRA_HOST])

    if source_table not in (WEATHER_TABLE, FAKER_TABLE, FASTF1_TABLE):
        return None

    session = cluster.connect(CASSANDRA_KEYSPACE)
    session.row_factory = dict_factory
    cqlquery = "SELECT * FROM " + source_table + ";"
    rows = session.execute(cqlquery)
    return pd.DataFrame(rows)


if __name__ == "__main__":
    action = sys.argv[1]
    target = sys.argv[2]
    targetfile = sys.argv[3]
    if action == "save":
        loadDF(targetfile, target)
    elif action == "get":
        getDF(target)