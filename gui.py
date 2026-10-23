import tkinter as tk
from tkinter import ttk, messagebox 
import json
import pika
from sendOrder import send_order

connection = None
channel = None
trade_queue = None
latest_prices = {} 

# RabbitMQ connection for completed trades
trade_connection = None
trade_channel = None
trade_queue = None


def check_trades():
    """Receive completed trades and update the GUI."""
    try:
        if trade_channel is not None:
            for _ in range(20):
                method, properties, body = trade_channel.basic_get(
                    queue=trade_queue,
                    auto_ack=True
                )

                if method is None:
                    break

                trade = json.loads(body) 
                print("GUI RECEIVED COMPLETED TRADE:", trade)
                
                symbol = trade["symbol"].upper()
                price = float(trade["price"])

                latest_prices[symbol] = price
                update_price()

    except (pika.exceptions.AMQPError, ValueError, KeyError) as error:
        print("Trade listener error:", error)

    root.after(500, check_trades)


def update_price():
    symbol = symbol_var.get()
    price = latest_prices.get(symbol)
    if price is None:
        price_label.config(text="No trades yet")
    else:
        price_label.config(text=f"${price:.2f}")
                        
def submit_order():
    trader = trader_entry.get().strip()
    symbol = symbol_var.get()
    action = action_var.get()

    try:
        price = float(price_entry.get())
        send_order(
            trader,
            "localhost",
            action,
            price,
            symbol
        )
        status_label.config(
            text=f"{action} order submitted"
        )

    except Exception as error:
        messagebox.showerror(
            "Order Error",
            str(error)
        )
        
root = tk.Tk()
root.title("PBT205 Trading System")
root.geometry("650x650")

# Trader name input
tk.Label(root, text="Trader Name:").pack(pady=5)

trader_entry = tk.Entry(root)
trader_entry.pack(pady=5)

# Stock selection
tk.Label(root, text="Select Stock:").pack(pady=5)

symbol_var = tk.StringVar(value="XYZ")

stock_menu = ttk.Combobox(
    root,
    textvariable=symbol_var,
    values=["XYZ", "ABC", "DEF"],
    state="readonly"
)
stock_menu.pack(pady=5)

# BUY or SELL selection
tk.Label(root, text="Order Action:").pack(pady=5)

action_var = tk.StringVar(value="BUY")

action_menu = ttk.Combobox(
    root,
    textvariable=action_var,
    values=["BUY", "SELL"],
    state="readonly"
)

action_menu.pack(pady=5)

# Order price input
tk.Label(root, text="Order Price ($):").pack(pady=5)

price_entry = tk.Entry(root)
price_entry.pack(pady=5)

# Fixed order quantity
tk.Label(root, text="Quantity: 100 shares").pack(pady=5)

# Send Order button
tk.Button(
    root,
    text="Send Order",
    command=submit_order,
    bg="#1565C0",
    fg="white",
    font=("Arial", 12, "bold")
).pack(pady=10)


# Latest completed trading price
tk.Label(
    root,
    text="Latest Completed Trading Price:",
    font=("Arial", 11, "bold")
).pack(pady=5)

price_label = tk.Label(
    root,
    text="No trades yet",
    font=("Arial", 18, "bold"),
    fg="green"
)
price_label.pack(pady=5)

# Order submission status
status_label = tk.Label(
    root,
    text="",
    font=("Arial", 10)
)
status_label.pack(pady=5)



# Connect GUI to completed trades
trade_connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost")
)

trade_channel = trade_connection.channel()

trade_channel.exchange_declare(
    exchange="trading",
    exchange_type="topic",
    durable=True
)

result = trade_channel.queue_declare(
    queue="",
    exclusive=True
)

trade_queue = result.method.queue

trade_channel.queue_bind(
    exchange="trading",
    queue=trade_queue,
    routing_key="trades"
)

root.after(500, check_trades)

# Start the GUI application
root.mainloop()



