import argparse
import datetime

import pandas as pd
import requests
from tqdm import tqdm

from config import *


def get_date_range(start_date, end_date):
    """
    returns a generator that yields a Date for each date between start_date and end_date.
    """

    for num_days in range((end_date - start_date).days):
        yield start_date + datetime.timedelta(num_days)


def get_station_discharge_on_date(date: str, station_id: str) -> None:
    """
    fetches mean hydrometric flow from the station with id `station_id` on date
    `date`.

    `date` must follow a valid iso format.
    """

    if not isinstance(date, datetime.date):
        date = datetime.date.fromisoformat(date)

    try:
        # GET data
        response = requests.get(HYDRO_API_URL.format(station_id, date.isoformat()))

        # raise HTTP errors as python exceptions
        response.raise_for_status()

        # return response
        return response.json()["properties"]["DISCHARGE"]  # TODO move labels to config

    except Exception as e:
        print(e)
        return e


def get_station_precipitation_on_date(date: str, station_id: str) -> None:
    """
    fetches daily precipitation amount from the station with id `station_id` on
    date `date`.

    `date` must be of the form "year-month-day"
    """

    # clean and separate date data
    if not isinstance(date, datetime.date):
        date = datetime.date.fromisoformat(date)

    try:
        # GET data
        response = requests.get(CLIMATE_API_URL.format(station_id, date.year, date.month, date.day))

        # raise HTTP errors as python exceptions
        response.raise_for_status()

        # return response
        return response.json()["properties"]["TOTAL_PRECIPITATION"]  # TODO move labels to config

    except Exception as e:
        print(e)


def generate_table(start_date, end_date, hdm_station, precipitation_station):
    """
    returns a pandas DataFrame of hydrometric daily mean and precipitation data
    from start_date to end_date.
    """

    # convert dates
    start_date = datetime.date.fromisoformat(start_date)
    end_date = datetime.date.fromisoformat(end_date)

    precipitation = []
    hdm = []
    dates = list(get_date_range(start_date, end_date))

    for date in tqdm(dates):
        precipitation.append(
            get_station_precipitation_on_date(date, station_id=precipitation_station)
        )
        hdm.append(get_station_discharge_on_date(date, station_id=hdm_station))

    return pd.DataFrame({"Date": dates, "Discharge": hdm, "Precipitation": precipitation})


def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--hydro-station", type=str, default="02DD014")
    parser.add_argument("--climate-station", type=str, default="6085680")

    parser.add_argument("start_date", type=str)
    parser.add_argument("end_date", type=str)

    parser.add_argument("-o", "--output", type=str, default=None)

    args = parser.parse_args()

    return args


def main():
    # parse args
    args = get_args()

    # generate data
    data = generate_table(args.start_date, args.end_date, args.hydro_station, args.climate_station)

    # write to file
    if args.output:
        with open(args.output) as out_file:
            data.to_csv(out_file)
    else:
        print(data)


if __name__ == "__main__":
    main()
