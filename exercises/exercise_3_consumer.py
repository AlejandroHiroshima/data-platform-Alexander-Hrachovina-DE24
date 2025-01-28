#CONSUMER
from quixstreams import Application

app = Application(
    broker_address="localhost:9092",
    consumer_group="show_order",
    auto_offset_reset="earliest",
)

order_topic = app.topic(name="orders", value_deserializer="json")

sdf = app.dataframe(topic=order_topic)

def show_orders(message):
    """
    Bearbetar och visar order-information
    """
    order = message
    output = {
        'order_id': order['order_id'],
        'customer': order['customer'],
        'total': order['total'],
        'products': [
            {
                'name': product['name'],
                'price': product['price'],
                'quantity': product['quantity']
            }
            for product in order['products']
        ]
    }
    return output

# Applicera transformationen
sdf = sdf.apply(show_orders)

# Skriv ut varje meddelande som tas emot
def print_message(row):
    print("\nNy order mottagen:")
    print(f"Order ID: {row['order_id']}")
    print(f"Kund: {row['customer']}")
    print("Produkter:")
    for product in row['products']:
        print(f"- {product['name']}: {product['quantity']}st à {product['price']} kr")
    print(f"Totalt: {row['total']} kr")
    print("---")

sdf.update(print_message)

if __name__ == '__main__':
    app.run()