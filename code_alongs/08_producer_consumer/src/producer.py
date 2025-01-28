from pathlib import Path
import json
from pprint import pprint
from quixstreams import Application

data_path = Path(__file__).parents[1] / "data"

# print(data_path)

# läser in jokes.json genom sin absoluta path
with open(data_path / "jokes.json", "r") as file:
    jokes = json.load(file)

# pprint(jokes)

# form av entry point för att interagera med Kafka, localhost:9092 is port mapped to broker container
app = Application(broker_address="localhost:9092", consumer_group="text-splitter")

# Skapar en topic
jokes_topic = app.topic(name="jokes", value_serializer="json")
# print(jokes_topic)


# Skapar vår producer
def main():
    with app.get_producer() as producer:
        # print(producer)

        for joke in jokes:

            kafka_msg = jokes_topic.serialize(key=joke["joke_id"], value=joke)

            print(f"produced message: key = {kafka_msg.key}, value = {kafka_msg.value}")

            producer.produce(topic="jokes", key= str(kafka_msg.key), value= kafka_msg.value)


#runt this code only when executin this script and not when importing this module
if __name__ == '__main__':
    # pprint(jokes)
    main()
