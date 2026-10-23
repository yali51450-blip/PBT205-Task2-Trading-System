
import argparse
import json
import pika

def send_order(trader, host, action, price, symbol="XYZ"):
    """Submit a BUY or SELL order to RabbitMQ."""

    trader = trader.strip()
    action = action.upper()
    symbol = symbol.strip().upper()

    if not trader:
        raise ValueError("Trader name is required")

    if action not in ("BUY", "SELL"):
        raise ValueError("Action must be BUY or SELL")

    if not symbol or not symbol.isalnum():
        raise ValueError("Invalid stock symbol")

    if not 0 < price < float("inf"):
        raise ValueError("Price must be a positive finite number")

    order = {
        "trader": trader,
        "symbol": symbol,
        "action": action,
        "quantity": 100,
        "price": price
    }

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

        channel.confirm_delivery()

        channel.basic_publish(
            exchange="trading",
            routing_key="Orders",
            body=json.dumps(order),
            mandatory=True,
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2
            )
        )

        print("\nORDER SUBMITTED SUCCESSFULLY")
        print(f"Trader: {trader}")
        print(f"Stock: {symbol}")
        print(f"Action: {action}")
        print("Quantity: 100 shares")
        print(f"Price: ${price:.2f}")
        print("Order accepted by RabbitMQ")

    finally:
        if connection.is_open:
            connection.close()

def main():
    parser = argparse.ArgumentParser(
        description="PBT205 Trading System"
    )

    parser.add_argument("trader")
    parser.add_argument("host")
    parser.add_argument(
        "action",
        type=str.upper,
        choices=["BUY", "SELL"]
    )
    parser.add_argument("quantity", type=int, choices=[100])
    parser.add_argument("price", type=float)
    parser.add_argument("--symbol", default="XYZ")

    args = parser.parse_args()

    try:
        send_order(
            args.trader,
            args.host,
            args.action,
            args.price,
            args.symbol
        )
    except (ValueError, pika.exceptions.AMQPError) as error:
        parser.exit(1, f"Error: {error}\n")

if __name__ == "__main__":
    main()
