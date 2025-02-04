# Importerar Kafka-biblioteket Quixstreams som hanterar strömning av data
# Application är huvudklassen som hanterar både producent och konsument-funktionalitet
from quixstreams import Application

# Skapar en Application-instans för att konsumera (ta emot) data från Kafka
# Viktiga parametrar som MÅSTE anges:
# - broker_address: Adressen där Kafka-servern körs (format: "host:port")
# - consumer_group: Unikt namn som identifierar denna grupp av konsumenter
#                  Flera konsumenter i samma grupp delar på arbetsbördan
# - auto_offset_reset: Bestämmer var läsningen ska börja när ingen position finns sparad
#                     "earliest" = börjar från första meddelandet
#                     "latest" = börjar från senaste meddelandet (standard)
app = Application(
    broker_address="localhost:9092",  # Standard Kafka-port
    consumer_group="show_order",      # Valfritt gruppnamn
    auto_offset_reset="earliest",     # Börja från första meddelandet
)

# Konfigurerar topic (kommunikationskanal) för att ta emot meddelanden
# Parametrar som MÅSTE anges:
# - name: Namnet på topic (måste matcha producentens topic)
# - value_deserializer: Hur inkommande data ska konverteras
#                      "json" konverterar JSON-strängar till Python-objekt
order_topic = app.topic(name="orders", value_deserializer="json")

# Skapar en streaming dataframe (sdf) för enklare databearbetning
# app.dataframe() skapar ett objekt som liknar pandas DataFrame men för streaming
# topic: anger vilken topic som ska strömmas
sdf = app.dataframe(topic=order_topic)

# Visar information om streaming dataframe-objektet
# Användbart för debugging och för att förstå datastrukturen
print(sdf)

# Funktion som bearbetar varje mottaget meddelande från Kafka
def show_orders(message):
    """
    Bearbetar och formaterar orderdata från Kafka-meddelanden

    Args:
        message (dict): Rådata från Kafka-meddelandet

    Returns:
        dict: Formaterad orderinformation med beräknade summor
    """
    # Beräknar total ordersumma med list comprehension
    # Multiplicerar pris med kvantitet för varje produkt och summerar
    total = sum(product['price'] * product['quantity'] for product in message['products'])

    # Skapar ny dictionary-struktur för formaterad data
    # Innehåller bara den information vi vill visa
    formatted_order = {
        'order_id': message['order_id'],    # Unikt order-ID
        'customer': message['customer'],     # Kundnamn
        'products': [],                      # Tom lista som fylls nedan
        'total': total,                     # Totalsumma
        'status': message['status']         # Orderstatus
    }

    # Bearbetar varje produkt i ordern
    # Lägger till formaterad produktinformation med delsummor
    for product in message['products']:
        formatted_order['products'].append({
            'name': product['name'],
            'price': product['price'],
            'quantity': product['quantity'],
            'subtotal': product['price'] * product['quantity']  # Delsumma per produkt
        })

    return formatted_order

# Applicerar show_orders på varje meddelande i dataframe
# sdf.apply() kör angiven funktion på varje rad/meddelande
sdf = sdf.apply(show_orders)

# Funktion för att skriva ut orderinformation i ett läsbart format
def print_formatted_order(order):
    """
    Skriver ut orderinformation i ett snyggt formaterat format

    Args:
        order (dict): Formaterad orderinformation från show_orders
    """
    # Skapar visuell separator med = tecken
    print("\n" + "="*50)

    # Skriver ut orderheader
    print(f"Order ID: {order['order_id']}")
    print(f"Kund: {order['customer']}")
    print(f"Status: {order['status']}")

    # Skriver ut produktsektion
    print("\nProdukter:")
    print("-"*50)

    # Loopar genom och skriver ut varje produkt
    for product in order['products']:
        print(f"- {product['name']}")
        print(f"  Antal: {product['quantity']}")
        # :.2f formaterar float med 2 decimaler
        print(f"  Pris per st: {product['price']:.2f} kr")
        print(f"  Delsumma: {product['subtotal']:.2f} kr")

    # Skriver ut totalsumma och avslutande separator
    print("-"*50)
    print(f"Totalsumma: {order['total']:.2f} kr")
    print("="*50 + "\n")

# Kopplar print_formatted_order till dataframe
# sdf.update() kör funktionen på varje uppdatering i strömmen
sdf.update(print_formatted_order)

# Standard Python idiom för att köra kod endast när filen körs direkt
# app.run() startar konsumenten och börjar lyssna efter meddelanden
if __name__ == '__main__':
    app.run()