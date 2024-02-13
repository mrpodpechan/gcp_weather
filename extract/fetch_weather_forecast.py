def fetch_weather_forecast(request):
    import requests
    import json
    import time
    import logging
    import csv
    import io

    from google.cloud import storage
    from google.cloud import secretmanager

    try:
        #client variables
        storage_client = storage.Client()
        secrets_client = secretmanager.SecretManagerServiceClient()

        #api variables
        lat = 33.17
        lon = 96.95
        part = 'alerts'  # excluded alerts for v1

        #id variables
        project_id = 'weather-data-podpechan'
        secret_id = 'weather_api'
        bucket_id = 'weather_payload'

        #fetch API key from secret manager
        name = f'projects/{project_id}/secrets/{secret_id}/versions/1'
        response = secrets_client.access_secret_version(request={"name": name})
        api_key = response.payload.data.decode("UTF-8")

        #fetch weather forecast data from openweathermap API
        r = requests.get(f'https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&exclude={part}&appid={api_key}')
        r.raise_for_status()
        status = "success"
        json_payload = r.json()

        #data grain variables
        current_data = json_payload['current']
        daily_data = json_payload['daily'][0]
        hourly_data = json_payload['hourly']

        #bucket variables
        bucket = storage_client.get_bucket(bucket_id)

        #csv filename variables
        csv_filename_current = f'{int(time.time())}_forecast_current.csv'
        csv_filename_daily = f'{int(time.time())}_forecast_daily.csv'
        csv_filename_hourly = f'{int(time.time())}_forecast_hourly.csv'

        #grain dictionaries
        weather_dict_current = {
            'id': int(time.time()),
            'lat': lat,
            'lon': lon,
            'tz': json_payload['timezone'],
            'date': time.strftime('%Y-%m-%d', time.localtime(current_data['dt'])),
            'units': 'standard',
            'current_temp': current_data['temp'],
            'feels_like': current_data['feels_like'],
            'pressure': current_data['pressure'],
            'humidity': current_data['humidity'],
            'dew_point': current_data['dew_point'],
            'uvi': current_data['uvi'],
            'clouds': current_data['clouds'],
            'visibility': current_data['visibility'],
            'wind_speed': current_data['wind_speed'],
            'wind_deg': current_data['wind_deg'],
            'wind_gust': current_data.get('wind_gust', None)
        }

        weather_dict_daily = {
            'id': int(time.time()),
            'lat': lat,
            'lon': lon,
            'tz': json_payload['timezone'],
            'units': 'standard',
            'sunrise': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(daily_data['sunrise'])),
            'sunset': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(daily_data['sunset'])),
            'moon_phase': daily_data['moon_phase'],
            'summary': daily_data['weather'][0]['description'],
            'day_temp': daily_data['temp']['day'],
            'min_temp': daily_data['temp']['min'],
            'max_temp': daily_data['temp']['max'],
            'night_temp': daily_data['temp']['night'],
            'eve_temp': daily_data['temp']['eve'],
            'morn_temp': daily_data['temp']['morn'],
            'feels_like_day': daily_data['feels_like']['day'],
            'feels_like_night': daily_data['feels_like']['night'],
            'feels_like_eve': daily_data['feels_like']['eve'],
            'feels_like_morn': daily_data['feels_like']['morn'],
            'pressure_day': daily_data['pressure'],
            'humidity_day': daily_data['humidity'],
            'dew_point_day': daily_data['dew_point'],
            'wind_speed_day': daily_data['wind_speed'],
            'wind_deg_day': daily_data['wind_deg'],
            'wind_gust_day': daily_data.get('wind_gust', None)
        }

        #store current in GCS as csv
        with io.StringIO() as csv_buffer_current:
            writer_current = csv.DictWriter(csv_buffer_current, fieldnames=weather_dict_current.keys())
            writer_current.writeheader()
            writer_current.writerow(weather_dict_current)

            csv_data_current = csv_buffer_current.getvalue().encode('utf-8')
            blob_current = bucket.blob(csv_filename_current)
            blob_current.upload_from_string(csv_data_current, content_type='text/csv')

        #store daily in GCS as csv
        with io.StringIO() as csv_buffer_daily:
            writer_daily = csv.DictWriter(csv_buffer_daily, fieldnames=weather_dict_daily.keys())
            writer_daily.writeheader()
            writer_daily.writerow(weather_dict_daily)

            csv_data_daily = csv_buffer_daily.getvalue().encode('utf-8')
            blob_daily = bucket.blob(csv_filename_daily)
            blob_daily.upload_from_string(csv_data_daily, content_type='text/csv')

        #hourly data dictionary
        for hourly_entry in hourly_data:
            weather_dict_hourly = {
                'id': int(time.time()),
                'lat': lat,
                'lon': lon,
                'tz': json_payload['timezone'],
                'temp': hourly_entry['temp'],
                'feels_like': hourly_entry['feels_like'],
                'pressure': hourly_entry['pressure'],
                'humidity': hourly_entry['humidity'],
                'dew_point': hourly_entry['dew_point'],
                'uvi': hourly_entry['uvi'],
                'clouds': hourly_entry['clouds'],
                'visibility': hourly_entry['visibility'],
                'wind_speed': hourly_entry['wind_speed'],
                'wind_deg': hourly_entry['wind_deg'],
                'wind_gust': hourly_entry.get('wind_gust', None),
                'weather_id': hourly_entry['weather'][0]['id'],
                'weather_main': hourly_entry['weather'][0]['main'],
                'weather_description': hourly_entry['weather'][0]['description'],
                'weather_icon': hourly_entry['weather'][0]['icon'],
                'pop': hourly_entry.get('pop', None)
            }

            #store hourly in GCS as csv
            with io.StringIO() as csv_buffer_hourly:
                writer_hourly = csv.DictWriter(csv_buffer_hourly, fieldnames=weather_dict_hourly.keys())
                writer_hourly.writeheader()
                writer_hourly.writerow(weather_dict_hourly)

                csv_data_hourly = csv_buffer_hourly.getvalue().encode('utf-8')
                blob_hourly = bucket.blob(csv_filename_hourly)
                blob_hourly.upload_from_string(csv_data_hourly, content_type='text/csv')

        return json.dumps({"status": "success", "in_process": status}), 200, {'Content-Type': 'application/json'}

    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
        return json.dumps({"status": "failure", "output": str(e)}), 500, {'Content-Type': 'application/json'}
