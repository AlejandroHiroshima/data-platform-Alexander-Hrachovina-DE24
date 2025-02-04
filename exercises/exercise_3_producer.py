# Importerar nödvändiga bibliotek
from pathlib import Path  # Hanterar filvägar på ett säkert sätt över olika operativsystem
import json              # Hanterar JSON-data (läsning/skrivning)
from pprint import pprint  # Pretty print - skriver ut data i ett läsbart format
from quixstreams import Application  # Kafka-bibliotek för strömhantering

# Path(__file__) ger sökvägen till nuvarande fil
# .parent ger föräldrarmappen
# / "data" lägger till "data" till sökvägen
data_path = Path(__file__).parent / "data"
print(f"datapath = {data_path}")

# Öppnar JSON-filen i read-mode ('r')
# with-statement säkerställer att filen stängs korrekt efter användning
with open(data_path / "orders.json", "r") as file:
    # json.load() konverterar JSON-text till Python-objekt (dict/list)
    orders = json.load(file)

# pprint formaterar utskriften med indenteringar och radbrytningar
# för bättre läsbarhet än vanlig print
pprint(orders)

# Skapar en Application-instans för Kafka
# Parametrar som MÅSTE anges:
# - broker_address: adressen till Kafka-servern (format: "host:port")
# - consumer_group: unikt namn för denna konsumentgrupp
app = Application(
    broker_address="localhost:9092",  # Standardport för Kafka
    consumer_group="show_order"       # Valfritt gruppnamn
)

# Skapar en topic (en kanal i Kafka)
# Parametrar som MÅSTE anges:
# - name: namnet på topic
# - value_serializer: hur data ska konverteras när det skickas
order_topic = app.topic(
    name="orders",        # Valfritt topic-namn
    value_serializer="json"  # Anger att data ska serialiseras som JSON
)

# Context manager för producer - säkerställer korrekt stängning
# app.get_producer() skapar en ny producer-instans
with app.get_producer() as producer:
    # Itererar genom varje order i vår data
    for order in orders:
        # Serialiserar data för Kafka
        # order_topic.serialize() konverterar Python-objekt till Kafka-meddelande
        # Parametrar:
        # - key: unik nyckel för meddelandet
        # - value: själva data som ska skickas
        kafka_msg = order_topic.serialize(key=order, value=order)

        # Itererar genom varje meddelande
        for order in kafka_msg:
            # Skriver ut orderinformation
            print(f"Input: {order}")
            print(f"Order ID: {order['order_id']}")
            print(f"Kund namn: {order['customer']}")

            # Extraherar datum och status
            order_date = order['order_date']
            order_status = order['status']
            total = 0

            # Bearbetar varje produkt i ordern
            for product in order['products']:
                name = product['name']
                unit_price = product['price']
                quantity = product['quantity']
                total_price = unit_price * quantity
                total += total_price

                # Anpassad utskrift beroende på kvantitet
                if quantity > 1:
                    print(f"- Produktnamn: {name} (Styckpris: {unit_price:.2f} kr, "
                          f"Totalt: {total_price:.2f} kr)")
                else:
                    print(f"- Produktnamn: {name} (Pris: {unit_price:.2f} kr)")
                print(f"- Antal: {quantity}")

            # Skriver ut ordersammanfattning
            print(f"Order date: {order_date}")
            print(f"Order status: {order_status}")
            print(f"Totalsumma: {total:.2f} kr")
            print("---")

        # Skickar meddelandet till Kafka
        # producer.produce() skickar data till angiven topic
        # Parametrar som MÅSTE anges:
        # - topic: namnet på topic att skicka till
        # - key: unik identifierare för meddelandet
        # - value: själva data som ska skickas
        producer.produce(
            topic="orders",
            key=str(kafka_msg.key),    # Måste vara string
            value=kafka_msg.value      # Redan serialiserat JSON
        )