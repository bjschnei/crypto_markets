import datetime
import json
import urllib.request
import time

SATOSHIS_PER_BITCOIN = 10**8
INDEX_SYMBOL = '.BXBT'
CONTRACT_EXPIRATION_MONTH = {
  'XBTH': 3,
  'XBTM': 6,
  'XBTU': 9,
  'XBTZ': 12,
}
EXPIRATION_DAY_OF_MONTH = 27


def GetDate(str_datetime):
  """Convert the string formatted date to a date object

  Expects format 2019-06-18T00:00:00.000Z
  """
  str_date = str_datetime[:str_datetime.find('T')]
  dt = datetime.datetime.strptime(str_datetime[:str_datetime.find('T')],
                                  '%Y-%m-%d')
  return dt.date()


def GetBTCDailyPrices(symbol, start_date, num_days):
  """Daily price is the closing price of the Bitmex .BXBT index.
  """
  # constants
  max_query_days = 500
  final_date = start_date + datetime.timedelta(days=num_days)

  # Query for all the days requested
  query_days = min((final_date - start_date).days, max_query_days)
  end_date = start_date + datetime.timedelta(days=query_days)
  prices = {}
  while start_date < final_date:
    start_str = start_date.isoformat()
    end_str = end_date.isoformat()
    url = ('https://www.bitmex.com/api/v1/trade/bucketed?binSize=1d&partial=false'
           '&symbol=%s&reverse=false&startTime=%s&endTime=%s') % (
           symbol, start_str, end_str)
    # don't make too many http requests
    time.sleep(2)
    with urllib.request.urlopen(url) as conn:
      daily_data = json.loads(conn.read())
      for daily in daily_data:
        prices[GetDate(daily['timestamp'])] = daily['close']
    if len(daily_data) == 0:
      # no more data for this symbol
      break
    start_date = start_date + datetime.timedelta(days=len(daily_data))
    query_days = min((final_date - start_date).days, max_query_days)
    end_date = start_date + datetime.timedelta(days=query_days)
  return prices


def GetDailyBasis(expiration_date, daily_prices, daily_index_prices):
  daily_basis = {}
  for day, price in daily_prices.items():
    expiry = expiration_date - day
    if expiry.days <= 0:
      continue
    try:
      daily_basis[day] = (
          (price / daily_index_prices[day] - 1) / (expiry.days / 365)
      )
    except KeyError:
      # No price for the given day
      daily_basis[day] = None
      continue
  return daily_basis


def GetContractExpirations(start_date, num_days):
  # Get the futures and expirations we are interested in.
  end_date = start_date + datetime.timedelta(days=num_days)
  years= list(range(start_date.year, end_date.year)) + [end_date.year]
  years_suffix = (year - 2000 for year in years)
  contract_expirations = {}
  for year in years_suffix:
    for future, month in CONTRACT_EXPIRATION_MONTH.items():
      contract_expirations[future + str(year)] = (
          datetime.date(month=month, day=EXPIRATION_DAY_OF_MONTH,
                        year=2000 + int(year)))
  return contract_expirations


def GetPrices(contract_expirations, start_date, num_days):
  # Get the index prices and all futures prices
  index_prices = GetBTCDailyPrices(INDEX_SYMBOL, start_date, num_days)
  futures_prices = {}
  for contract, expiry in contract_expirations.items():
    days_to_expiry = (expiry - start_date).days
    if days_to_expiry > 0:
      futures_prices[contract] = GetBTCDailyPrices(
          contract, start_date, days_to_expiry)
  return index_prices, futures_prices


def GetBasisRates(contract_expirations, futures_prices, index_prices):
  # Get the daily basis for each future
  futures_basis = {}
  for contract, prices in futures_prices.items():
    futures_basis[contract] = GetDailyBasis(
        contract_expirations[contract], prices, index_prices)
  return futures_basis


if __name__ == '__main__':
  num_days = 500
  end_date = datetime.date.today() - datetime.timedelta(days=1)
  start_date = end_date - datetime.timedelta(days=num_days)
  contract_expirations = GetContractExpirations(start_date, num_days)
  index_prices, futures_prices = GetPrices(contract_expirations,
                                           start_date, num_days)
  futures_basis = GetBasisRates(contract_expirations, futures_prices,
                                index_prices)
  for contract, basis in futures_basis.items():
    print(contract)
    print(basis)
    print(futures_prices[contract])
