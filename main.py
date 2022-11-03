# pandas-bt.py

import sys

import requests
import pandas as pd

pd.options.mode.chained_assignment = None


def get_ohlc_data() -> pd.DataFrame:
    """Return a DataFrame of OHLC data"""

    market = "BTC-GBP"
    granularity = 3600
    resp = requests.get(
        f"https://api.pro.coinbase.com/products/{market}/candles?granularity={granularity}"
    )
    df = pd.DataFrame.from_dict(resp.json())

    df.columns = ["epoch", "open", "high", "close", "low", "volume"]
    tsidx = pd.DatetimeIndex(
        pd.to_datetime(df["epoch"], unit="s"), dtype="datetime64[ns]"
    )
    df.set_index(tsidx, inplace=True)
    df = df.drop(columns=["epoch"])
    df.index.names = ["ts"]
    df["date"] = df.index

    return df[["date", "open", "high", "close", "low", "volume"]].iloc[::-1]


def add_ema1226(df: pd.DataFrame) -> pd.DataFrame:
    """Add EMA12 and EMA26"""

    df["ema12"] = df["close"].ewm(span=12, adjust=False).mean()
    df["ema26"] = df["close"].ewm(span=26, adjust=False).mean()

    df.loc[df["ema12"] > df["ema26"], "ema12gtema26"] = True
    df["ema12gtema26"].fillna(False, inplace=True)
    df.loc[df["ema12"] < df["ema26"], "ema12ltema26"] = True
    df["ema12ltema26"].fillna(False, inplace=True)

    df["ema12gtema26co"] = df.ema12gtema26.ne(df.ema12gtema26.shift())
    df.loc[df["ema12gtema26"] == False, "ema12gtema26co"] = False
    df["ema12ltema26co"] = df.ema12ltema26.ne(df.ema12ltema26.shift())
    df.loc[df["ema12ltema26"] == False, "ema12ltema26co"] = False

    return df


def set_buy_signals(df: pd.DataFrame) -> pd.DataFrame:
    """Set buy signals"""

    df["buy_signal"] = 0

    df.loc[df["ema12gtema26co"] == True, "buy_signal"] = 1
    df["buy_signal"].fillna(0, inplace=True)

    return df


def set_sell_signals(df: pd.DataFrame) -> pd.DataFrame:
    """Set buy signals"""

    df["sell_signal"] = 0

    df.loc[df["ema12ltema26co"] == True, "sell_signal"] = 1
    df["sell_signal"].fillna(0, inplace=True)

    return df


def main() -> int:
    """Backtest a strategy using pandas"""

    # initialisation values
    market = "BTC-GBP"
    granularity = 3600
    balance_base = 0
    balance_quote = 1000
    buy_order_quote = 1000
    is_order_open = False
    orders = []

    df = get_ohlc_data()
    df = add_ema1226(df)
    df = set_buy_signals(df)
    df = set_sell_signals(df)

    for index, row in df.iterrows():
        if row["buy_signal"] and is_order_open == 0:
            is_order_open = 1

            buy_amount = buy_order_quote / row["close"]
            balance_base += buy_amount
            balance_quote -= buy_order_quote

            order = {
                "timestamp": str(row["date"]),
                "market": market,
                "granularity": granularity,
                "balance_open": balance_quote,
                "buy_order_quote": buy_order_quote,
                "buy_order_base": buy_amount
            }

        if row["sell_signal"] and is_order_open == 1:
            is_order_open = 0

            sell_value = buy_amount * row["close"]
            balance_quote += sell_value
            balance_base -= buy_amount

            order["sell_order_quote"] = sell_value
            order["balance_close"] = balance_quote
            order["profit"] = order["sell_order_quote"] - order["buy_order_quote"]
            order["margin"] = (order["profit"] / order["buy_order_quote"]) * 100

            orders.append(order)

    df_orders = pd.DataFrame(orders)
    print(df_orders)

    return 0


if __name__ == "__main__":
    sys.exit(main())
