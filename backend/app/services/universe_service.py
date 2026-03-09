def load_tickers():
    tickers = []

    with open("app/data/all_tickers.txt", "r") as f:
        for line in f:
            ticker = line.strip()

            if (
                ticker
                and "^" not in ticker
                and "/" not in ticker
                and ticker.isalpha()
            ):
                tickers.append(ticker)

    return tickers