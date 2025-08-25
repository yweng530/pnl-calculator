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

def main():
    csv_path, scheme = parse_arg()

    print("TIMESTAMP,SYMBOL,PNL")
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
            

if __name__ == "__main__":
    main()
