# Import the dependencies.
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, text, func

from flask import Flask, jsonify

import datetime as dt
import pandas as pd


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
base = automap_base()
# reflect the tables
base.prepare(autoload_with=engine)

# Save references to each table
stations = base.classes.station
measure = base.classes.measurement

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)



#################################################
# Flask Routes
#################################################
@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/precipitation<br/>"
        f"/api/stations<br/>"
        f"/api/tobs<br/>"
        f"/api/start/date<br/>"
        f"/api/start-end/start date/end date"
    )

@app.route("/api/precipitation")
def precipitation_func():
    session = Session(engine)
    recentData = session.query(measure.date).order_by(measure.date.desc()).first()
    dateList = recentData[0].split('-')
    year_prior = dt.datetime(int(dateList[0]), int(dateList[1]), int(dateList[2])) - dt.timedelta(days=365)
    with engine.connect() as conn:
        result = conn.execute(text(f'Select date, prcp from measurement')).fetchall()
    prcpDF = pd.DataFrame(result, columns = ['Date','PRCP'])
    prcpDF['Date'] = pd.to_datetime(prcpDF['Date'])
    prcpDF = prcpDF.loc[prcpDF['Date'] >= year_prior]
    prcpDF = prcpDF.dropna()
    prcpDF = prcpDF.sort_values(by='Date')
    prcpDict = prcpDF.to_dict(orient='records')

    return jsonify(prcpDict)

@app.route("/api/stations")
def stations_func():
    session = Session(engine)

    results = session.query(stations.station,stations.name,stations.latitude,stations.longitude,stations.elevation).all()
    session.close()

    all_stations = []
    for station, name, latitude, longitude, elevation in results:
        station_dict = {}
        station_dict['station'] = station
        station_dict['name'] = name
        station_dict['latitude'] = latitude
        station_dict['longitude'] = longitude
        station_dict['elevation'] = elevation
        all_stations.append(station_dict)
    
    return jsonify(all_stations)

@app.route("/api/tobs")
def tobs_func():
    with engine.connect() as conn:
        stationDate = conn.execute(text('Select Date from measurement where station = "USC00519281" order by date desc')).first()
        stationData = conn.execute(text('Select Station, Date, TOBS from measurement where station = "USC00519281" order by date desc')).all()

    dateListStation = stationDate[0].split('-')
    year_prior_station = dt.datetime(int(dateListStation[0]), int(dateListStation[1]), int(dateListStation[2])) - dt.timedelta(days=365)

    stationDF = pd.DataFrame(stationData, columns = ['Station', 'Date','TOBS'])
    stationDF['Date'] = pd.to_datetime(stationDF['Date'])
    stationDF = stationDF.loc[stationDF['Date'] >= year_prior_station]
    stationDF = stationDF.dropna()
    stationDict = stationDF.to_dict(orient='records')

    return jsonify(stationDict)

@app.route("/api/start/<start_route>")
def start_func(start_route):
    session = Session(engine)
    with engine.connect() as conn:
        result = conn.execute(text(f'Select date, prcp from measurement')).fetchall()
    prcp_start = pd.DataFrame(result, columns = ['Date','PRCP'])
    prcp_start['Date'] = pd.to_datetime(prcp_start['Date'])
    prcp_start = prcp_start.loc[prcp_start['Date'] == start_route]
    prcp_start = prcp_start.dropna()
    prcp_start = prcp_start.sort_values(by='Date')
    prcp_start_Dict = prcp_start.to_dict(orient='records')

    return jsonify(prcp_start_Dict)

@app.route("/api/start-end/<start_route>/<end_route>")
def end_func(start_route,end_route):
    session = Session(engine)
    with engine.connect() as conn:
        result = conn.execute(text(f'Select date, prcp from measurement')).fetchall()
    prcp_end = pd.DataFrame(result, columns = ['Date','PRCP'])
    prcp_end['Date'] = pd.to_datetime(prcp_end['Date'])
    prcp_end = prcp_end.loc[(prcp_end['Date'] >= start_route)&(prcp_end['Date'] <= end_route)]
    prcp_end = prcp_end.dropna()
    prcp_end = prcp_end.sort_values(by='Date')
    prcp_end_Dict = prcp_end.to_dict(orient='records')

    return jsonify(prcp_end_Dict)