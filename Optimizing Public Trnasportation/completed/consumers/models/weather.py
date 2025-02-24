"""Contains functionality related to Weather"""
import logging
import json

logger = logging.getLogger(__name__)

class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        try:
            value = json.loads(message.value())
            if "temperature" in value and "status" in value:
                self.temperature = value["temperature"]
                self.status = value["status"]
                logger.info(f"Updated weather: {self.temperature}°F, {self.status}")
            else:
                logger.warning("Weather message missing required fields")
        except Exception as e:
            logger.error(f"Failed to process weather message: {e}")
