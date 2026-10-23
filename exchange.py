import pika
import json
import sys

# Store unmatched orders
buy_orders = []
sell_orders = []

def publish_trade(channel, buyer, seller, price):
    trade = {
        "symbol": buyer["symbol"],
        "quantity": 100,
        "price": price,
        "buyer": buyer["trader"],
        "seller": seller["trader"]
    }
   
    channel.basic_publish(
        exchange="trading",
        routing_key="trades",
        body=json.dumps(trade),
        properties=pika.BasicProperties(
            delivery_mode=2
        )
    )
    print("Trade completed:", trade)

def process_order(channel, method, properties, body):
    try:
        order = json.loads(body)
        side = order["action"].upper()
        symbol = order["symbol"].upper()
        price = float(order["price"])

        print("Order received:", order)

        if side == "BUY":
            matching_sellers = [
                seller for seller in sell_orders
                if seller["symbol"] == symbol
                and seller["price"] <= price
                and seller["trader"] != order["trader"]
            ]

            if matching_sellers:
                seller = min(
                    matching_sellers,
                    key=lambda item: item["price"]
                )

                publish_trade(
                    channel,
                    buyer=order,
                    seller=seller,
                    price=seller["price"]
                )

                sell_orders.remove(seller)
            else:
                buy_orders.append(order)
                print("BUY order waiting for a seller")

        elif side == "SELL":
            matching_buyers = [
                buyer for buyer in buy_orders
                if buyer["symbol"] == symbol
                and buyer["price"] >= price
                and buyer["trader"] != order["trader"]
            ]

            if matching_buyers:
                buyer = max(
                    matching_buyers,
                    key=lambda item: item["price"]
                )

                publish_trade(
                    channel,
                    buyer=buyer,
                    seller=order,
                    price=buyer["price"]
  )
                buy_orders.remove(buyer)
            else:
                sell_orders.append(order)
                print("SELL order waiting for a buyer")
    except (ValueError, KeyError, TypeError) as error:
        print("Invalid order:", error)
        return

def main():
    host = sys.argv[1] if len(sys.argv) > 1 else "localhost"
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=host)
    )

    try:
        channel = connection.channel()

        channel.exchange_declare(
            exchange="trading",
            exchange_type="topic",
            durable=True
        )

        channel.queue_declare(
            queue="Orders",
            durable=True
        )

        channel.queue_bind(
            exchange="trading",
            queue="Orders",
            routing_key="Orders"
        )

        channel.queue_declare(
            queue="trades",
            durable=True
        )

        channel.queue_bind(
            exchange="trading",
            queue="trades",
            routing_key="trades"
        )

        channel.basic_qos(prefetch_count=1)

        channel.basic_consume(
            queue="Orders",
            on_message_callback=process_order,
            auto_ack=True
        )

        print("Trading Exchange Running")
        print("Waiting for BUY and SELL orders...")

        channel.start_consuming()

    except KeyboardInterrupt:
        print("Trading exchange stopped")

    finally:
        if connection.is_open:
            connection.close()

if __name__ == "__main__":
    main()

    
