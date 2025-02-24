"""Methods pertaining to weather data"""
from enum import IntEnum
import json
import logging
from pathlib import Path
import random
import requests

from models.producer import Producer

logger = logging.getLogger(__name__)

class Weather(Producer):
    """Defines a simulated weather model"""

    status = IntEnum(
        "status", "sunny partly_cloudy cloudy windy precipitation", start=0
    )

    rest_proxy_url = "http://localhost:8082"

    key_schema = None
    value_schema = None

    winter_months = {0, 1, 2, 3, 10, 11}
    summer_months = {6, 7, 8}

    def __init__(self, month):
        super().__init__(
            "org.chicago.cta.weather.v1",
            key_schema=Weather.key_schema,
            value_schema=Weather.value_schema,
            num_partitions=3,  # Adjust based on expected throughput
            num_replicas=1,  # Set higher for redundancy in production
        )

        self.status = Weather.status.sunny
        self.temp = 70.0
        if month in Weather.winter_months:
            self.temp = 40.0
        elif month in Weather.summer_months:
            self.temp = 85.0

        if Weather.key_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_key.json") as f:
                Weather.key_schema = json.load(f)

        if Weather.value_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_value.json") as f:
                Weather.value_schema = json.load(f)

    def _set_weather(self, month):
        """Simulates changes in weather conditions"""
        mode = 0.0
        if month in Weather.winter_months:
            mode = -1.0
        elif month in Weather.summer_months:
            mode = 1.0
        self.temp += min(max(-20.0, random.triangular(-10.0, 10.0, mode)), 100.0)
        self.status = random.choice(list(Weather.status))

    def run(self, month):
        """Posts a weather event to Kafka REST Proxy"""
        self._set_weather(month)

        logger.info(f"Producing weather event: {self.temp}°F, {self.status.name}")

        headers = {"Content-Type": "application/vnd.kafka.avro.v2+json"}

        payload = {
            "key_schema": json.dumps(Weather.key_schema),
            "value_schema": json.dumps(Weather.value_schema),
            "records": [
                {
                    "key": {"timestamp": self.time_millis()},
                    "value": {"temperature": self.temp, "status": self.status.name}
                }
            ]
        }

        response = requests.post(
            f"{Weather.rest_proxy_url}/topics/org.chicago.cta.weather.v1",
            headers=headers,
            data=json.dumps(payload),
        )

        try:
            response.raise_for_status()
            logger.debug("Weather event sent successfully")
        except requests.exceptions.HTTPError as e:
            logger.error(f"Failed to send weather event: {e}")
