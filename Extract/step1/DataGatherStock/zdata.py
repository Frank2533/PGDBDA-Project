import click
import pickle as pk
import csv
from jugaad_trader import Zerodha


@click.command()
@click.option("--instrument", "-i", help='Instrument name "NSE:INFY"', type=str)
@click.option("--from", "-f", "from_", help="from date yyyy-mm-dd")
@click.option("--to", "-t", help="to date yyyy-mm-dd")
@click.option("--interval", "-n", default="day", help="Data interval eg. minute, day")
@click.option("--output", "-o", help="Output file name")
def main(instrument, from_, to, interval, output):
    #print(instrument, from_, to, interval, output)
    kite = Zerodha()

    kite.set_access_token()
    q = kite.ltp(instrument)
    token = q[instrument]['instrument_token']

    data = kite.historical_data(token, from_, to, interval)
    with open(output, 'w') as fp:
        writer = csv.DictWriter(fp, ["date", "open", "high", "low", "close", "volume"])
        writer.writeheader()
        writer.writerows(data)


if __name__ == "__main__":
    main()
