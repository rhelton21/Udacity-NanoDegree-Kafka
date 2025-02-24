"""Defines trends calculations for stations"""
import logging
import faust

logger = logging.getLogger(__name__)

# Faust will ingest records from Kafka in this format
class Station(faust.Record, serializer="json"):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool

# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record, serializer="json"):
    station_id: int
    station_name: str
    order: int
    line: str

# Define the Faust application
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")

# Define the input Kafka Topic (from Kafka Connect)
topic = app.topic("org.chicago.cta.stations", value_type=Station)

# Define the output Kafka Topic
out_topic = app.topic("org.chicago.cta.stations.transformed", partitions=1)

# Define a Faust Table to store transformed data
table = app.Table(
    "transformed_stations_table",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic,
)

@app.agent(topic)
async def process_stations(stations):
    """Transform Station records into TransformedStation format"""
    async for station in stations:
        # Determine the correct line color based on boolean fields
        line_color = "unknown"
        if station.red:
            line_color = "red"
        elif station.blue:
            line_color = "blue"
        elif station.green:
            line_color = "green"

        # Create a new TransformedStation record
        transformed = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=line_color
        )

        # Send transformed station data to the output Kafka topic
        await out_topic.send(value=transformed)

if __name__ == "__main__":
    app.main()
