"""Contains functionality related to Stations"""
import json
import logging

logger = logging.getLogger(__name__)

class Station:
    """Defines the Station Model"""

    def __init__(self, station_id, station_name, order):
        """Creates a Station Model"""
        self.station_id = station_id
        self.station_name = station_name
        self.order = order
        self.dir_a = None
        self.dir_b = None
        self.num_turnstile_entries = 0

    @classmethod
    def from_message(cls, value):
        """Given a Kafka Station message, creates and returns a station"""
        try:
            return Station(value["station_id"], value["station_name"], value["order"])
        except KeyError as e:
            logger.error(f"Missing field in station message: {e}")
            return None

    def handle_departure(self, direction):
        """Removes a train from the station"""
        if direction == "a":
            self.dir_a = None
        elif direction == "b":
            self.dir_b = None
        else:
            logger.warning(f"Invalid departure direction: {direction}")

    def handle_arrival(self, direction, train_id, train_status):
        """Unpacks arrival data and updates station status"""
        status_dict = {"train_id": train_id, "status": train_status.replace("_", " ")}
        if direction == "a":
            self.dir_a = status_dict
        elif direction == "b":
            self.dir_b = status_dict
        else:
            logger.warning(f"Invalid arrival direction: {direction}")

    def process_message(self, json_data):
        """Handles arrival and turnstile messages"""
        if "COUNT" in json_data:
            self.num_turnstile_entries = json_data["COUNT"]
        else:
            logger.warning("Turnstile message missing COUNT field")
