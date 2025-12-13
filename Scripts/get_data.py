import requests
import datetime
import pandas as pd
from tqdm import tqdm


def daterange(start_date, end_date):
    """
    returns a generator that yields a Date for each date between start_date and end_date.
    """

    for num_days in range((end_date-start_date).days):
        yield start_date + datetime.timedelta(num_days)


def get_station_discharge_on_date(date: str, station_id: str = "02DD014",) -> None:
    """
    fetches mean hydrometric flow from the station with id `station_id` on date
    `date`.

    `date` must follow a valid iso format.
    """

    if not isinstance(date, datetime.date):
        date = datetime.date.fromisoformat(date)

    try:
        # create URL
        URL = f"https://api.weather.gc.ca/collections/hydrometric-daily-mean/items/{station_id}.{date.isoformat()}"

        # GET data from URL
        response = requests.get(URL)

        # raise HTTP errors as python exceptions
        response.raise_for_status()

        # return response
        return response.json()["properties"]["DISCHARGE"]

    except Exception as e:
        print(e)
        return e


def get_station_precipitation_on_date(date: str, station_id: str = "6085680") -> None:
    """
    fetches daily precipitation amount from the station with id `station_id` on
    date `date`.

    `date` must be of the form "year-month-day"
    """

    # clean and separate date data
    if not isinstance(date, datetime.date):
        date = datetime.date.fromisoformat(date)

    try:
        # create URL
        URL = f"https://api.weather.gc.ca/collections/climate-daily/items/{station_id}.{date.year}.{date.month}.{date.day}"
        
        # GET data from URL
        response = requests.get(URL)

        # raise HTTP errors as python exceptions
        response.raise_for_status()

        # return response
        return response.json()["properties"]["TOTAL_PRECIPITATION"]

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
    dates = list(daterange(start_date, end_date))

    for date in tqdm(dates):
        precipitation.append(get_station_precipitation_on_date(date, station_id=precipitation_station))
        hdm.append(get_station_discharge_on_date(date, station_id=hdm_station))

    return pd.DataFrame({"Date": dates, "Discharge": hdm, "Precipitation": precipitation})
