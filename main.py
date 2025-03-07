import threading
import time
from heapq import *
import random
from datetime import datetime

NUM_TICKER = 1024
random.seed(12)


class Engine():
    orderbook = [[[], []] for _ in range(NUM_TICKER)]
    locks = [threading.Lock() for _ in range(NUM_TICKER)]

    def __init__(self):
        pass

    def addOrder(self, OrderType: int, TickerSymbol: int, Quantity: int, Price: float):
        dt = datetime.now()
        with self.locks[TickerSymbol]:
            if OrderType == 1:
                # Sell order book is min heap (To get ask price)
                heappush(self.orderbook[TickerSymbol][OrderType],
                         (dt, Quantity, Price))
            else:
                # Buy order book is max heap (To get bid price)
                heappush(self.orderbook[TickerSymbol][OrderType],
                         (dt, Quantity, -Price))

    def simulateOrder(self):
        while True:
            for ticker in range(NUM_TICKER):
                order_type = random.randint(0, 1)
                qty = random.randint(1, 100)
                price = round(random.uniform(40, 60), 2)
                self.addOrder(order_type, ticker, qty, price)
                # print(
                #     f'{[datetime.now()]} order submitted {(order_type, ticker, qty, price)}')
            time.sleep(0.01)

    def matchOrder(self, TickerSymbol: int):
        # pick the highest buy bid and pick lowest sell ask and try matching them.
        with self.locks[TickerSymbol]:
            sell_book = self.orderbook[TickerSymbol][1]
            buy_book = self.orderbook[TickerSymbol][0]

            while True:
                if min(len(sell_book), len(buy_book)) == 0:
                    break
                bid_ts, bid_qty, bid_price = heappop(buy_book)
                ask_ts, ask_qty, ask_price = heappop(sell_book)
                bid_price *= -1

                if bid_price >= ask_price:
                    trade_qty = min(bid_qty, ask_qty)
                    print(
                        f"Market: {TickerSymbol} Bid: {bid_qty} @ {bid_price} Ask: {ask_qty} @ {ask_price} Executed: {trade_qty}")
                    bid_qty -= trade_qty
                    ask_qty -= trade_qty

                    if bid_qty > 0:
                        heappush(buy_book, (bid_ts, bid_qty, -bid_price))
                    if ask_qty > 0:
                        heappush(sell_book, (ask_ts, ask_qty, ask_price))

                else:
                    # print("No orders to match")
                    break


if __name__ == '__main__':
    engine = Engine()

    # Start simulateOrder in a separate thread
    simulate_thread = threading.Thread(target=engine.simulateOrder)
    simulate_thread.start()

    sp = datetime.now()
    while True:
        for i in range(NUM_TICKER):
            engine.matchOrder(i)
        time.sleep(1)
