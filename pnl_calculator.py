import sys
import csv
from collections import deque

def parse_arg():
    if len(sys.argv) != 3:
        sys.stderr.write("num of arguments should be 3")
    file_path = sys.argv[1]
    scheme = sys.argv[2].lower()
    if scheme not in ("fifo", "lifo"):
        sys.stderr.write("Error: scheme must be 'fifo' or 'lifo'")
    return file_path, scheme

def calculate_pnl(hashmap, symbol, type, price, qty, scheme):
    opposite = 'S' if type == 'B' else 'B'
    # look up trades for the opposite type
    trades = hashmap[symbol][opposite] # queue of (qty, price)
    pnl = 0.0
    qty_matched = 0
    remain = qty
    while trades and remain > 0:
        if scheme == 'fifo':
            prev_qty, prev_price = trades.popleft()
        else:
            prev_qty, prev_price = trades.pop()
        qty_took = min(prev_qty, remain)
        if type == 'S':
            pnl += (price - prev_price) * qty_took
        else:
            pnl += (prev_price - price) * qty_took
        qty_matched += qty_took
        remain -= qty_took
        prev_qty -= qty_took
        # put the remaining quantity back to the queue
        if prev_qty > 0:
            if scheme == 'fifo':
                trades.appendleft((prev_qty, prev_price))
            else:
                trades.append((prev_qty, prev_price))
    return pnl, qty_matched, remain

def main():
    csv_path, scheme = parse_arg()

    writer = csv.writer(sys.stdout, lineterminator="\n")
    writer.writerow(["TIMESTAMP", "SYMBOL", "PNL"])
    hashmap = {} # hashmap[symbol] gives {"B": [list of (quantity, price)], "S": [list of (quantity, price)]}

    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            time = row["TIMESTAMP"].strip()
            symbol = row["SYMBOL"].strip()
            type = row["BUY_OR_SELL"].strip().upper()
            price = float(row["PRICE"])
            qty = int(row["QUANTITY"])

            if type not in ("B", "S"):
                sys.stderr.write(f"invalid entry for BUY_OR_SELL: {type}")
                continue
            if symbol not in hashmap:
                hashmap[symbol] = {"B": deque(), "S": deque()}

            pnl, qty_matched, remain = calculate_pnl(hashmap, symbol, type, price, qty, scheme)
            if qty_matched > 0:
                writer.writerow([time, symbol, f"{pnl:.2f}"])
            if remain > 0:
                hashmap[symbol][type].append((remain, price))
            

if __name__ == "__main__":
    main()
