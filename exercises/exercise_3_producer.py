from pathlib import Path
import json
from pprint import pprint
from quixstreams import Application

data_path = Path(__file__).parent / "data"

with open(data_path / "orders.json", "r") as file:
    orders = json.load(file)

app = Application(broker_address="localhost:9092", consumer_group="show_order")
order_topic = app.topic(name="orders", value_serializer="json")

def main():
    with app.get_producer() as producer:
        for order in orders:
            # Konvertera order till JSON-sträng och sedan till bytes
            order_json = json.dumps(order)
            order_bytes = order_json.encode('utf-8')

            # Konvertera key till string och sedan till bytes
            key_bytes = str(order['order_id']).encode('utf-8')

            producer.produce(
                topic="orders",
                key=key_bytes,
                value=order_bytes
            )

            # Skriv ut order-information
            print(f"Input: {order}")
            print(f"Order ID: {order['order_id']}")
            print(f"Kund namn: {order['customer']}")
            order_date = order['order_date']
            order_status = order['status']
            total = 0

            for product in order['products']:
                name = product['name']
                price = product['price']
                quantity = product['quantity']
                if quantity > 1:
                    price = price * quantity
                total += price

                print(f"- Produktnamn: {name}")
                print(f"- Pris: {price}")
                print(f"- Antal: {quantity}")

            print(f"Order date: {order_date}")
            print(f"Order status: {order_status}")
            print(f"Totalsumma: {total}")
            print("---")

if __name__ == '__main__':
    main()