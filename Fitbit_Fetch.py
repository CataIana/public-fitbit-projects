# %%
import base64
import requests
from schedule import every, run_pending
import time
import json
import pytz
import logging
import os
from requests.exceptions import ConnectionError
from datetime import datetime, timedelta
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.client.exceptions import InfluxDBError
from typing import Optional, Tuple
from dotenv import load_dotenv

# %% [markdown]
# ## Variables

# %%
# Load .env file
load_dotenv()
FITBIT_LANGUAGE = os.environ.get("FITBIT_LANGUAGE")
TOKEN_FILE_PATH = os.environ.get("TOKEN_FILE_PATH", "token_file.json")
INFLUXDB_HOST = os.environ.get("INFLUXDB_HOST")
INFLUXDB_PORT = os.environ.get("INFLUXDB_PORT")
INFLUXDB_BUCKET = os.environ.get("INFLUXDB_BUCKET")
INFLUXDB_ORGANIZATION = os.environ.get("INFLUXDB_ORGANIZATION")
INFLUXDB_TOKEN = os.environ.get("INFLUXDB_TOKEN")
CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
DEVICENAME = os.environ.get("DEVICENAME")
AUTO_DATE_RANGE = os.environ.get("AUTO_DATE_RANGE") == "true"
AUTO_UPDATE_DATE_RANGE = int(os.environ.get("AUTO_UPDATE_DATE_RANGE"))
LOCAL_TIMEZONE = os.environ.get("TIMEZONE")
MAX_RETRIES = 3
EXPIRED_TOKEN_MAX_RETRY = 5
API_URI = "https://api.fitbit.com"

# %% [markdown]
# ## Logging setup

# %%

logging.basicConfig(
    level=logging.DEBUG,
    format="%(funcName)-26s || %(levelname)-5s || %(message)s"
)

class FitBitFetch:
    def __init__(self):
        self.points: list[dict] = []
        try:
            self.db_client = InfluxDBClient(
                f"{INFLUXDB_HOST}{f':{INFLUXDB_PORT}' if INFLUXDB_PORT else ''}", token=INFLUXDB_TOKEN, org=INFLUXDB_ORGANIZATION, timeout=30000)
        except InfluxDBError as err:
            logging.error("Unable to connect with influxdb database! Aborted")
            raise InfluxDBError(f"InfluxDB connection failed: {str(err)}")
        self.timezone = pytz.timezone(LOCAL_TIMEZONE) if LOCAL_TIMEZONE else pytz.timezone(self.fitbit_data_request(f"1/user/-/profile.json")["user"]["timezone"])
        self.end_date: datetime = datetime.now()
        self.start_date: datetime = self.end_date - timedelta(days=AUTO_UPDATE_DATE_RANGE)

    def end_date_str(self) -> str:
        return self.end_date.strftime("%Y-%m-%d")

    def start_date_str(self) -> str:
        return self.start_date.strftime("%Y-%m-%d")

    def update_working_dates(self):
        self.end_date = datetime.now()
        self.start_date = self.end_date - timedelta(days=AUTO_UPDATE_DATE_RANGE)

    def startup_update(self):
        """Call the functions one time as a startup update OR do switch to bulk update mode"""
        if AUTO_DATE_RANGE:
            date_list = [(self.start_date + timedelta(days=i)).strftime("%Y-%m-%d")
                        for i in range((self.end_date - self.start_date).days + 1)]
            if len(date_list) > 3:
                logging.warn("Auto schedule update is not meant for more than 3 days at a time, please consider lowering the auto_update_date_range variable to aviod rate limit hit!")
            for date_str in date_list:
                self.get_intraday_data_limit_1d(date_str=date_str)  # 2 queries x number of dates ( default 2)
            self.get_daily_data_limit_30d() # 3 Queries
            self.get_daily_data_limit_100d() # 1 Query
            self.get_daily_data_limit_365d() # 8 Queries
            self.get_daily_data_limit_none() # 1 Query
            self.get_battery_level() # 1 Query
            self.fetch_latest_activities() # 1 Query
        else:
            input_start_date = input("Enter start date in YYYY-MM-DD format : ")
            input_end_date = input("Enter end date in YYYY-MM-DD format : ")
            self.start_date = datetime.strptime(input_start_date, "%Y-%m-%d")
            self.end_date = datetime.strptime(input_end_date, "%Y-%m-%d")
            # Bulk update
            date_list = [(self.start_date + timedelta(days=i)).strftime("%Y-%m-%d")
                        for i in range((self.end_date - self.start_date).days + 1)]

            def yield_dates_with_gap(date_list: list[str], gap: int):
                start_index = -1*gap
                while start_index < len(date_list)-1:
                    start_index = start_index + gap
                    end_index = start_index+gap
                    if end_index > len(date_list) - 1:
                        end_index = len(date_list) - 1
                    if start_index > len(date_list) - 1:
                        break
                    yield (date_list[start_index], date_list[end_index])

            def do_bulk_update(funcname: function, start_date: str, end_date: str):
                funcname(start_date, end_date)
                run_pending()

            self.fetch_latest_activities(date_list[-1])
            self.write_points()
            do_bulk_update(self.get_daily_data_limit_none, date_list[0], date_list[-1])
            for date_range in yield_dates_with_gap(date_list, 360):
                do_bulk_update(self.get_daily_data_limit_365d, date_range[0], date_range[1])
            for date_range in yield_dates_with_gap(date_list, 98):
                do_bulk_update(self.get_daily_data_limit_100d, date_range[0], date_range[1])
            for date_range in yield_dates_with_gap(date_list, 28):
                do_bulk_update(self.get_daily_data_limit_30d, date_range[0], date_range[1])
            for single_day in date_list:
                do_bulk_update(self.get_intraday_data_limit_1d, single_day, [
                            ('heart', 'HeartRate_Intraday', '1sec'), ('steps', 'Steps_Intraday', '1min')])

            logging.info(
                f"Success: Bulk update complete for {self.start_date_str()} to {self.end_date_str()}")

    def setup_schedulers(self):
        logging.info("Creating schedulers")
        every(1).minute.do(self.write_points)
        every(3).minutes.do(self.get_intraday_data_limit_1d)
        every(20).minutes.do(self.get_battery_level)
        every(1).hour.do(self.fetch_latest_activities)
        every(3).hours.do(self.get_daily_data_limit_30d)
        every(4).hours.do(self.get_daily_data_limit_100d)
        every(6).hours.do(self.get_daily_data_limit_365d)
        every(6).hours.do(self.get_daily_data_limit_none)

    def fitbit_data_request(self, endpoint: str, headers: dict[str, str] = {}, params: dict[str, str] = {}, data: dict[str, str] = {}, request_type: str = "GET") -> Optional[dict]:
        retry_attempts = 0
        logging.debug(f"Requesting endpoint: {endpoint}")
        while retry_attempts < MAX_RETRIES:  # Unlimited Retry attempts
            headers = headers or  {
                "Authorization": f"Bearer {self.get_access_token()}",
                "Accept": "application/json",
                'Accept-Language': FITBIT_LANGUAGE
            }
            try:
                response = requests.request(
                    request_type, f"{API_URI}/{endpoint.lstrip('/')}", headers=headers, params=params, data=data)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    # Ratelimited, retry after header response + a little extra
                    retry_after = int(response.headers["Retry-After"]) + 15
                    logging.warning(f"Hit API ratelimit. Retrying in {retry_after}s...")
                    time.sleep(retry_after)
                elif response.status_code == 401:
                    # Access token may have expired, wait and retry
                    logging.warning(f"Unauthorized error: {response.text}")
                    time.sleep(30)
                # Fitbit server is down or not responding ( most likely ):
                elif response.status_code in [500, 502, 503, 504]:
                    logging.warning(f"Error {response.status_code} encountered. Retrying after 120s....")
                    time.sleep(120)
                else:
                    logging.error(f"Fitbit API request failed. Status code: {response.status_code} {response.text}")
                    response.raise_for_status()
                    return

            except ConnectionError as e:
                logging.error(f"Connection error, retrying in 30 seconds: {str(e)}")
            retry_attempts += 1
            time.sleep(30)
        logging.error(f"Retry limit exceeded for {endpoint}. Please debug - {response.text}")

    def refresh_fitbit_token(self, refresh_token) -> Tuple[str, str]:
        logging.info("Attempting to refresh tokens...")
        url = f"oauth2/token"
        headers = {
            "Authorization": f"Basic {base64.b64encode((f'{CLIENT_ID}:{CLIENT_SECRET}').encode()).decode()}",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token
        }
        json_data = self.fitbit_data_request(
            url, headers=headers, data=data, request_type="post")
        access_token = json_data["access_token"]
        new_refresh_token = json_data["refresh_token"]
        expiry_time = int(time.time()) + json_data["expires_in"] - 300
        tokens = {
            "access_token": access_token,
            "refresh_token": new_refresh_token,
            "expiry_timestamp": expiry_time,
        }
        with open(TOKEN_FILE_PATH, "w", encoding="utf-8") as tfile:
            tfile.write(json.dumps(tokens, indent=4))
        logging.info("Refreshed fitbit token")
        return access_token, new_refresh_token

    def get_access_token(self) -> str:
        try:
            with open(TOKEN_FILE_PATH, "r") as file:
                tokens = json.load(file)
                access_token, refresh_token, expiry_timestamp = tokens.get("access_token"), tokens.get("refresh_token"), tokens.get("expiry_timestamp", 0)
        except FileNotFoundError:
            refresh_token = input(
                "No token file found. Please enter a valid refresh token : ")
        # If token has expired
        if expiry_timestamp < int(time.time()):
            access_token, refresh_token = self.refresh_fitbit_token(refresh_token)
        return access_token

    def write_points(self):
        if len(self.points) > 0:
            try:
                write_api = self.db_client.write_api(write_options=SYNCHRONOUS)
                write_api.write(
                    INFLUXDB_BUCKET,
                    INFLUXDB_ORGANIZATION,
                    self.points
                )
                self.points.clear()
                logging.info("Successfully updated influxdb database with new points")
            except InfluxDBError as err:
                logging.error(f"Unable to connect with influxdb database! {str(err)}")

    def get_battery_level(self):
        """Get last synced battery level of device"""
        device = self.fitbit_data_request(f"1/user/-/devices.json")[0]
        if device != None:
            self.points.append({
                "measurement": "DeviceBatteryLevel",
                "time": self.timezone.localize(datetime.fromisoformat(device['lastSyncTime'])).astimezone(pytz.utc).isoformat(),
                "fields": {
                    "value": float(device['batteryLevel'])
                }
            })
            logging.info(f"Recorded battery level for {DEVICENAME}")
        else:
            logging.error(f"Recording battery level failed : {DEVICENAME}")

    def get_intraday_data_limit_1d(self, date_str: str = None, measurement_list = [("heart", "HeartRate_Intraday", "1sec"), ("steps", "Steps_Intraday", "1min")]):
        """For intraday detailed data, max possible range in one day."""
        date_str = date_str or self.end_date_str()
        for measurement in measurement_list:
            res = self.fitbit_data_request(f"1/user/-/activities/{measurement[0]}/date/{date_str}/1d/{measurement[2]}.json")
            data = res.get(f"activities-{measurement[0]}-intraday", {}).get("dataset")
            if data:
                for value in data:
                    log_time = datetime.fromisoformat(f"{date_str}T{value['time']}")
                    utc_time = self.timezone.localize(log_time).astimezone(pytz.utc).isoformat()
                    self.points.append({
                        "measurement":  measurement[1],
                        "time": utc_time,
                        "tags": {
                            "Device": DEVICENAME
                        },
                        "fields": {
                            "value": int(value["value"])
                        }
                    })
                logging.info(
                    f"Recorded {measurement[1]} intraday for date {date_str}")
            else:
                logging.error(
                    f"Recording failed: {measurement[1]} intraday for date {date_str}")

    def get_daily_data_limit_30d(self, start_date: str = None, end_date: str = None):
        """Max range is 30 days, records BR, SPO2 Intraday, skin temp and HRV - 4 queries"""
        start_date = start_date or self.start_date_str()
        end_date = end_date or self.end_date_str()
        res_hrv = self.fitbit_data_request(f"1/user/-/hrv/date/{start_date}/{end_date}.json")
        hrv_data_list = res_hrv.get("hrv")
        if hrv_data_list:
            for data in hrv_data_list:
                log_time = datetime.fromisoformat(f"{data['dateTime']}T00:00:00")
                utc_time = self.timezone.localize(log_time).astimezone(pytz.utc).isoformat()
                self.points.append({
                    "measurement":  "HRV",
                    "time": utc_time,
                    "tags": {
                        "Device": DEVICENAME
                    },
                    "fields": {
                        "dailyRmssd": data["value"]["dailyRmssd"],
                        "deepRmssd": data["value"]["deepRmssd"]
                    }
                })
            logging.info(
                f"Recorded HRV for date {start_date} to {end_date}")
        else:
            logging.error(
                f"Recording failed HRV for date {start_date} to {end_date}")

        res_br = self.fitbit_data_request(f"1/user/-/br/date/{start_date}/{end_date}.json")
        br_data_list = res_br.get("br")
        if br_data_list:
            for data in br_data_list:
                log_time = datetime.fromisoformat(f"{data['dateTime']}T00:00:00")
                utc_time = self.timezone.localize(
                    log_time).astimezone(pytz.utc).isoformat()
                self.points.append({
                    "measurement":  "BreathingRate",
                    "time": utc_time,
                    "tags": {
                        "Device": DEVICENAME
                    },
                    "fields": {
                        "value": data["value"]["breathingRate"]
                    }
                })
            logging.info(
                f"Recorded BR for date {start_date} to {end_date}")
        else:
            logging.error(
                f"Recording failed : BR for date {start_date} to {end_date}")

        res_skin = self.fitbit_data_request(f"1/user/-/temp/skin/date/{start_date}/{end_date}.json")
        skin_temp_data_list = res_skin.get("tempSkin")
        if skin_temp_data_list:
            for temp_record in skin_temp_data_list:
                log_time = datetime.fromisoformat(
                    f"{temp_record['dateTime']}T00:00:00")
                utc_time = self.timezone.localize(
                    log_time).astimezone(pytz.utc).isoformat()
                self.points.append({
                    "measurement":  "Skin Temperature Variation",
                    "time": utc_time,
                    "tags": {
                        "Device": DEVICENAME
                    },
                    "fields": {
                        "RelativeValue": temp_record["value"]["nightlyRelative"]
                    }
                })
            logging.info(
                f"Recorded Skin Temperature Variation for date {start_date} to {end_date}")
        else:
            logging.error(
                f"Recording failed: Skin Temperature Variation for date {start_date} to {end_date}")

        spo2_data_list = self.fitbit_data_request(f"1/user/-/spo2/date/{start_date}/{end_date}/all.json")
        if spo2_data_list:
            for days in spo2_data_list:
                data = days["minutes"]
                for record in data:
                    log_time = datetime.fromisoformat(record["minute"])
                    utc_time = self.timezone.localize(
                        log_time).astimezone(pytz.utc).isoformat()
                    self.points.append({
                        "measurement":  "SPO2_Intraday",
                        "time": utc_time,
                        "tags": {
                            "Device": DEVICENAME
                        },
                        "fields": {
                            "value": float(record["value"]),
                        }
                    })
            logging.info(
                f"Recorded SPO2 intraday for date {start_date} to {end_date}")
        else:
            logging.error(
                f"Recording failed: SPO2 intraday for date {start_date} to {end_date}")
    
    def get_daily_data_limit_100d(self, start_date: str = None, end_date: str = None):
        """Only for sleep data - limit 100 days - 1 query"""
        start_date = start_date or self.start_date_str()
        end_date = end_date or self.end_date_str()
        res_sleep = self.fitbit_data_request(f"1.2/user/-/sleep/date/{start_date}/{end_date}.json")
        sleep_data = res_sleep.get("sleep")
        if sleep_data:
            for record in sleep_data:
                log_time = datetime.fromisoformat(record["startTime"])
                utc_time = self.timezone.localize(
                    log_time).astimezone(pytz.utc).isoformat()
                try:
                    minutesLight = record['levels']['summary']['light']['minutes']
                    minutesREM = record['levels']['summary']['rem']['minutes']
                    minutesDeep = record['levels']['summary']['deep']['minutes']
                except:
                    minutesLight = record['levels']['summary']['asleep']['minutes']
                    minutesREM = record['levels']['summary']['restless']['minutes']
                    minutesDeep = 0

                self.points.append({
                    "measurement":  "Sleep Summary",
                    "time": utc_time,
                    "tags": {
                        "Device": DEVICENAME,
                        "isMainSleep": record["isMainSleep"],
                    },
                    "fields": {
                        'efficiency': record["efficiency"],
                        'minutesAfterWakeup': record['minutesAfterWakeup'],
                        'minutesAsleep': record['minutesAsleep'],
                        'minutesToFallAsleep': record['minutesToFallAsleep'],
                        'minutesInBed': record['timeInBed'],
                        'minutesAwake': record['minutesAwake'],
                        'minutesLight': minutesLight,
                        'minutesREM': minutesREM,
                        'minutesDeep': minutesDeep
                    }
                })

                sleep_level_mapping = {'wake': 3, 'rem': 2, 'light': 1,
                                    'deep': 0, 'asleep': 1, 'restless': 2, 'awake': 3}
                for sleep_stage in record['levels']['data']:
                    log_time = datetime.fromisoformat(sleep_stage["dateTime"])
                    utc_time = self.timezone.localize(
                        log_time).astimezone(pytz.utc).isoformat()
                    self.points.append({
                        "measurement":  "Sleep Levels",
                        "time": utc_time,
                        "tags": {
                            "Device": DEVICENAME,
                            "isMainSleep": record["isMainSleep"],
                        },
                        "fields": {
                            'level': sleep_level_mapping[sleep_stage["level"]],
                            'duration_seconds': sleep_stage["seconds"]
                        }
                    })
                wake_time = datetime.fromisoformat(record["endTime"])
                utc_wake_time = self.timezone.localize(
                    wake_time).astimezone(pytz.utc).isoformat()
                self.points.append({
                    "measurement":  "Sleep Levels",
                    "time": utc_wake_time,
                    "tags": {
                        "Device": DEVICENAME,
                        "isMainSleep": record["isMainSleep"],
                    },
                    "fields": {
                        'level': sleep_level_mapping['wake'],
                        'duration_seconds': None
                    }
                })
            logging.info(
                f"Recorded Sleep data for date {start_date} to {end_date}")
        else:
            logging.error(
                f"Recording failed : Sleep data for date {start_date} to {end_date}")
    
    def get_daily_data_limit_365d(self, start_date: str = None, end_date: str = None):
        """Max date range 1 year, records HR zones, Activity minutes and Resting HR - 4 + 3 + 1 + 1 = 9 queries"""
        start_date = start_date or self.start_date_str()
        end_date = end_date or self.end_date_str()
        activity_minutes_list = [
            "minutesSedentary", "minutesLightlyActive", "minutesFairlyActive", "minutesVeryActive"]
        for activity_type in activity_minutes_list:
            res_activities_minutes = self.fitbit_data_request(f"1/user/-/activities/tracker/{activity_type}/date/{start_date}/{end_date}.json")
            activity_minutes_data_list = res_activities_minutes.get(f"activities-tracker-{activity_type}")
            if activity_minutes_data_list:
                for data in activity_minutes_data_list:
                    log_time = datetime.fromisoformat(
                        f"{data['dateTime']}T00:00:00")
                    utc_time = self.timezone.localize(
                        log_time).astimezone(pytz.utc).isoformat()
                    self.points.append({
                        "measurement": "Activity Minutes",
                        "time": utc_time,
                        "tags": {
                            "Device": DEVICENAME
                        },
                        "fields": {
                            activity_type: int(data["value"])
                        }
                    })
                logging.info(
                    f"Recorded {activity_type} for date {start_date} to {end_date}")
            else:
                logging.error(
                    F"Recording failed : {activity_type} for date {start_date} to {end_date}")

        activity_others_list = ["distance", "calories", "steps"]
        for activity_type in activity_others_list:
            res_activities_other = self.fitbit_data_request(f"1/user/-/activities/tracker/{activity_type}/date/{start_date}/{end_date}.json")
            activity_others_data_list = res_activities_other.get(f"activities-tracker-{activity_type}")
            if activity_others_data_list:
                for data in activity_others_data_list:
                    log_time = datetime.fromisoformat(
                        f"{data['dateTime']}T00:00:00")
                    utc_time = self.timezone.localize(
                        log_time).astimezone(pytz.utc).isoformat()
                    activity_name = "Total Steps" if activity_type == "steps" else activity_type
                    self.points.append({
                        "measurement": activity_name,
                        "time": utc_time,
                        "tags": {
                            "Device": DEVICENAME
                        },
                        "fields": {
                            "value": float(data["value"])
                        }
                    })
                logging.info(
                    f"Recorded {activity_name} for date {start_date} to {end_date}")
            else:
                logging.error(
                    f"Recording failed: {activity_name} for date {start_date} to {end_date}")

        res_activities = self.fitbit_data_request(f"1/user/-/activities/heart/date/{start_date}/{end_date}.json")
        HR_zones_data_list = res_activities["activities-heart"]
        if HR_zones_data_list:
            for data in HR_zones_data_list:
                log_time = datetime.fromisoformat(f"{data['dateTime']}T00:00:00")
                utc_time = self.timezone.localize(
                    log_time).astimezone(pytz.utc).isoformat()
                self.points.append({
                    "measurement": "HR zones",
                    "time": utc_time,
                    "tags": {
                        "Device": DEVICENAME
                    },
                    "fields": {
                        "Normal": data["value"]["heartRateZones"][0]["minutes"],
                        "Fat Burn":  data["value"]["heartRateZones"][1]["minutes"],
                        "Cardio":  data["value"]["heartRateZones"][2]["minutes"],
                        "Peak":  data["value"]["heartRateZones"][3]["minutes"]
                    }
                })
                if "restingHeartRate" in data["value"]:
                    self.points.append({
                        "measurement":  "RestingHR",
                        "time": utc_time,
                        "tags": {
                            "Device": DEVICENAME
                        },
                        "fields": {
                            "value": data["value"]["restingHeartRate"]
                        }
                    })
            logging.info(
                f"Recorded RHR and HR zones for date {start_date} to {end_date}")
        else:
            logging.error(
                f"Recording failed : RHR and HR zones for date {start_date} to {end_date}")

    def get_daily_data_limit_none(self, start_date: str = None, end_date: str = None):
        """Records SPO2 single days for the whole given period - 1 query"""
        start_date = start_date or self.start_date_str()
        end_date = end_date or self.end_date_str()
        data_list = self.fitbit_data_request(f"1/user/-/spo2/date/{start_date}/{end_date}.json")
        if data_list:
            for data in data_list:
                log_time = datetime.fromisoformat(f"{data['dateTime']}T00:00:00")
                utc_time = self.timezone.localize(
                    log_time).astimezone(pytz.utc).isoformat()
                self.points.append({
                    "measurement":  "SPO2",
                    "time": utc_time,
                    "tags": {
                        "Device": DEVICENAME
                    },
                    "fields": {
                        "avg": data["value"]["avg"],
                        "max": data["value"]["max"],
                        "min": data["value"]["min"]
                    }
                })
            logging.info(
                f"Recorded Avg SPO2 for date {start_date} to {end_date}")
        else:
            logging.error(
                f"Recording failed: Avg SPO2 for date {start_date} to {end_date}")
    
    def fetch_latest_activities(self, end_date: str = None):
        """Fetches latest activities from record ( upto last 100 )"""
        end_date = end_date or self.end_date_str()
        recent_activities_data = self.fitbit_data_request(f"1/user/-/activities/list.json", params={
                                                        'beforeDate': end_date, 'sort': 'desc', 'limit': 50, 'offset': 0})
        if recent_activities_data:
            for activity in recent_activities_data['activities']:
                fields = {}
                if 'activeDuration' in activity:
                    fields['ActiveDuration'] = int(activity['activeDuration'])
                if 'averageHeartRate' in activity:
                    fields['AverageHeartRate'] = int(activity['averageHeartRate'])
                if 'calories' in activity:
                    fields['calories'] = int(activity['calories'])
                if 'duration' in activity:
                    fields['duration'] = int(activity['duration'])
                if 'distance' in activity:
                    fields['distance'] = float(activity['distance'])
                if 'steps' in activity:
                    fields['steps'] = int(activity['steps'])
                starttime = datetime.fromisoformat(
                    activity['startTime'].strip("Z"))
                utc_time = starttime.astimezone(pytz.utc).isoformat()
                self.points.append({
                    "measurement": "Activity Records",
                    "time": utc_time,
                    "tags": {
                        "ActivityName": activity['activityName']
                    },
                    "fields": fields
                })
            logging.info(
                f"Fetched 50 recent activities before date {end_date}")
        else:
            logging.error(
                f"Fetching 50 recent activities failed: before date {end_date}")

fitbit = FitBitFetch()
fitbit.startup_update()

if __name__ == "__main__":
    if AUTO_DATE_RANGE:
        fitbit.setup_schedulers()
        try:
            while True:
                run_pending()
                time.sleep(30)
                if datetime.now().day != fitbit.end_date.day:
                    fitbit.update_working_dates()
        except KeyboardInterrupt:
            pass
        finally:
            fitbit.write_points()
