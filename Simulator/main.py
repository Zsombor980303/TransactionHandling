import csv
import io
import json
import random
import time
import uuid
from datetime import datetime, timezone
import requests  # pip install requests
import pika

# A kész 1000 soros törzsadatbázisunk közvetlen linkje
MASTER_DATA_URL = "https://raw.githubusercontent.com/Zsombor980303/TransactionHandling/refs/heads/main/fintech_master_data.csv"

# RabbitMQ infrastructure configuration constants
RABBITMQ_HOST = 'localhost' #from docker-compose.yml
RABBITMQ_PORT = 5672 #from docker-compose.yml
EXCHANGE_NAME = 'fintech.transactions.exchange'
# 3 queues inside RabbitMQ, depending on the currency --> hungarian, foreign, or faulty/invalid
QUEUE_HUF = 'fintech.transactions.huf'
QUEUE_FX = 'fintech.transactions.fx'
QUEUE_DLQ = 'fintech.transactions.dead_letter'

# 3 routing keys. Later on, we will use this to tell Python and Docker, which way (queue) we want to send the data
KEY_HUF = 'tx.currency.huf'
KEY_FX = 'tx.currency.fx'
KEY_INVALID = 'tx.currency.invalid'

def load_master_data_from_web():
    #Letölti az 1000 soros éles CSV törzsadatot a webről és szótárak listájává alakítja.
    print("Törzsadatok letöltése a távoli szerverről...")
    try:
        # Kapcsolódás 10 másodperces időtúllépéssel (timeout)
        response = requests.get(MASTER_DATA_URL, timeout=10)
        # Ez azonnal kivételt dob, ha a szerver hibát ad vissza
        response.raise_for_status()

        # CSV feldolgozás a memóriában
        csv_file = io.StringIO(response.text) #python will treat our plain text as a csv file, eventhough it is not existing phisically
        csv_reader = csv.DictReader(csv_file, delimiter=';') #This is an iterator, we make a plan how to read the data (with ; separator, and the first row is the header)
        master_data_list = list(csv_reader) #now we read data, and python creating a dictionary from every row paired up with the header. When finished, it will create a huge list with dictionaries inside

        print(f"[+] {len(master_data_list)} egyedi ügyfél sikeresen betöltve a memóriába!")
        return master_data_list

    except requests.exceptions.Timeout:
        print("[-] Hiba: A szerver nem válaszolt időben (Timeout).")
        return []
    except requests.exceptions.HTTPError as http_err:
        print(f"[-] HTTP hiba történt: {http_err}")
        return []
    except requests.exceptions.RequestException as req_err:
        print(f"[-] Hálózati hiba lépett fel: {req_err}")
        return []
    except Exception as general_error:
        print(f"[-] Váratlan hiba történt a feldolgozás során: {general_error}")
        return []


def start_simulation():
    # Fetch master profiles from the single source of truth (GitHub)
    master_data = load_master_data_from_web()
    if not master_data:
        print("[-] Aborting simulation due to missing master data profiles.")
        return

    # Establish connection to the local RabbitMQ Message Broker
    try:
        # Establish and configure connection between python and  Docker (RabbitMQ in this case)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT)
        )
        channel = connection.channel() # Create virtual channel. Using more virtual channel inside the main connection is better in terms of resource and time efficiency

        # "exchange_type = topic" will make our data handling intelligent. With this, we can create more queue and route, so we can send data to separate ways depending on the currency.
        # "durable = true" means we save this logic/structure as well in our hardware.
        channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='topic', durable=True)

        # Create all 3 structural queues automatically
        channel.queue_declare(queue=QUEUE_HUF, durable=True)
        channel.queue_declare(queue=QUEUE_FX, durable=True)
        channel.queue_declare(queue=QUEUE_DLQ, durable=True)
        # Create the 3 routes paired with the proper queue
        channel.queue_bind(exchange=EXCHANGE_NAME, queue=QUEUE_HUF, routing_key=KEY_HUF)
        channel.queue_bind(exchange=EXCHANGE_NAME, queue=QUEUE_FX, routing_key=KEY_FX)
        channel.queue_bind(exchange=EXCHANGE_NAME, queue=QUEUE_DLQ, routing_key=KEY_INVALID)

        print(f"Successfully connected to RabbitMQ Broker on port {RABBITMQ_PORT}.")
    except Exception as broker_err:
        print(f"Failed to initialize RabbitMQ connection pipeline: {broker_err}")
        return

    # Counter to simulate an invalid data error on purpose
    loop_counter = 0

    try:
        while True:
            loop_counter += 1
            # Pick a random transaction row from our generated data collection
            client = random.choice(master_data)

            # Simulate normal transactional distribution with a 4% chance of high-value fraud patterns
            amount = round(random.uniform(500, 75000), 2)
            if random.random() > 0.96:
                # Later on, we will "investigate" these transactions in our C# code
                amount = round(random.uniform(150000, 2500000), 2)

            # Need to save the exact transaction date with milisec, and with a Z in the end, which indicates UTC timezone
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            currency_code = client["currency"]

            # Data manipulation / Fault injection --> Aim is to test our invalid key logic/route
            if loop_counter % 50 == 0:
                currency_code = "XYZ"

            # Check currency and decide accordingly which route/queue we send it later
            if currency_code == "HUF":
                active_routing_key = KEY_HUF
            elif currency_code in ["EUR", "USD"]:
                active_routing_key = KEY_FX
            else:
                active_routing_key = KEY_INVALID

            # Create our full transaction data row in json format
            transaction_event = {
                # Generate a globally unique UUIDv4 to act as an idempotency key
                "transaction_id": str(uuid.uuid4()),  # Idempotency key for .NET pipeline processing --> We will check in our C# code if this unique ID is already existing in our database. If yes, we will not do anything with it again, otherwise it would cause double payment
                "account_id": client["account_id"],
                "client_name": client["client_name"],
                "timestamp": timestamp,
                "transaction_type": random.choice(["PAYMENT", "TRANSFER", "WITHDRAWAL"]),
                "amount": amount,
                "currency": currency_code,
                "merchant_details": {
                    "merchant_name": client["merchant_name"],
                    "category_code": client["category_code"],
                    "location": client["merchant_location"]
                },
                "security_telemetry": {
                    "client_ip": client["client_ip"],
                    "device_fingerprint": client["device_fingerprint"],
                    "is_vpn": random.choice([True, False, False, False, False])
                }
            }

            # Publish to the Exchange using the dynamically calculated routing key.
            # The broker will read this tag and drop it into the correct queue automatically.
            message_body = json.dumps(transaction_event, ensure_ascii=False)
            channel.basic_publish(
                exchange=EXCHANGE_NAME,
                routing_key=active_routing_key,
                body=message_body,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Hardens data persistence onto the disk volume
                    content_type='application/json'
                )
            )

            print(
                f"[x] Ingested -> Key: {active_routing_key} | ID: {transaction_event['transaction_id']} | {transaction_event['amount']} {transaction_event['currency']}")

            # Simulate transaction traffic with minor sleep variation intervals (10ms to 50ms)
            time.sleep(random.uniform(0.01, 0.05))

    except KeyboardInterrupt:
        print("\n[*] Ingestion engine gracefully shut down by user.")
    # This part will be executed no matter what. Prevent Resource leak.
    finally:
        if connection.is_open:
            connection.close()

if __name__ == "__main__":
    start_simulation()