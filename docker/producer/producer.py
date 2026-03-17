import time
import requests
import json
import os
from kafka import KafkaProducer

# Wait 15 seconds for Kafka to be ready
time.sleep(15)

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# --- CONFIG ---
latitude = 41.385
longitude = 2.173

OPENAQ_API_KEY = os.environ.get("OPENAQ_API_KEY")
spain_country_id = 67
search_city = "Barcelona"

while True:
    try:
        # -------------------------
        # 1 WEATHER API
        # -------------------------
        weather_url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&current_weather=true"
        weather_res = requests.get(weather_url)

        if weather_res.status_code == 200:
            weather_data = weather_res.json()["current_weather"]

            producer.send("weather-barcelona", weather_data)
            print("Weather sent:", weather_data)

        # -------------------------
        # 2 AIR QUALITY API
        # -------------------------
        air_url = f"https://api.openaq.org/v3/locations?countries_id={spain_country_id}&limit=100"
        air_res = requests.get(air_url, headers={"X-API-Key": OPENAQ_API_KEY})

        if air_res.status_code == 200:
            data = air_res.json()["results"]

            barcelona_locs = [
                loc for loc in data
                if isinstance(loc["locality"], str)
                and search_city.lower() in loc["locality"].lower()
            ]

            producer.send("airquality-barcelona", barcelona_locs)
            print("Air quality sent:", barcelona_locs)

        else:
            print("Air API failed:", air_res.status_code)

    except Exception as e:
        print("Error:", e)

    # APIs of low update frecuency
    time.sleep(60)