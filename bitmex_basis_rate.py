import collections
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
ContractDetails = collections.namedtuple(
    'ContractDetails', ['listing', 'expiry'])


def GetDate(str_datetime):
  """Convert the string formatted date to a date object

  Expects format 2019-06-18T00:00:00.000Z
  """
  str_date = str_datetime[:str_datetime.find('T')]
  dt = datetime.datetime.strptime(str_datetime[:str_datetime.find('T')],
                                  '%Y-%m-%d')
  return dt.date()


def GetDailyData(base_url, start_date, num_days, on_data):
  """Get the daily data from the base url endpoint.

  Args:
    base_url: API url to call that can take start and end dates.
    start_date: datetime to start getting data
    num_days: number of days to add to start_date to use as end date
    on_data: function which takes as an argument the json results of the api
             and returns the last date of data received or None if no data.
  """
  final_date = start_date + datetime.timedelta(days=num_days)
  prices = {}
  end_str = final_date.isoformat()
  while start_date <= final_date:
    start_str = start_date.isoformat()
    url = base_url + '&startTime=%s&endTime=%s' % (start_str, end_str)
    # don't make too many http requests
    time.sleep(2)
    with urllib.request.urlopen(url) as conn:
      daily_data = json.loads(conn.read())
    last_date = on_data(daily_data)
    if last_date is None:
      break
    start_date = last_date + datetime.timedelta(days=1)


def GetBTCDailyPrices(symbol, start_date, num_days):
  base_url = ('https://www.bitmex.com/api/v1/trade/bucketed?binSize=1d'
              '&symbol=%s') % symbol
  daily_prices = {}
  def UpdatePrices(daily_data):
    daily_prices.update(
        {GetDate(daily['timestamp']): daily['close'] for daily in daily_data})
    return None if not daily_data else GetDate(daily_data[-1]['timestamp'])
  GetDailyData(base_url, start_date, num_days, UpdatePrices)
  return daily_prices


def GetDailyFunding(symbol, start_date, num_days):
  base_url = ('https://www.bitmex.com/api/v1/funding?symbol=%s') % symbol
  daily_funding = {}
  def UpdateFunding(daily_data):
    daily_funding.update(
        {GetDate(daily['timestamp']): daily['fundingRateDaily']
         for daily in daily_data})
    return None if not daily_data else GetDate(daily_data[-1]['timestamp'])
  GetDailyData(base_url, start_date, num_days, UpdateFunding)
  return daily_funding


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


def GetContractDetails(start_date, num_days):
  # Get the futures and expirations we are interested in.
  end_date = start_date + datetime.timedelta(days=num_days)
  years= list(range(start_date.year, end_date.year)) + [end_date.year]
  years_suffix = (year - 2000 for year in years)
  contract_details = {}
  for year in years_suffix:
    for future, month in CONTRACT_EXPIRATION_MONTH.items():
      symbol = future + str(year)
      url = 'https://www.bitmex.com/api/v1/instrument?symbol=' + symbol
      time.sleep(2)
      with urllib.request.urlopen(url) as conn:
        details = json.loads(conn.read())[0]
      contract_details[future + str(year)] = ContractDetails(
          GetDate(details['listing']), GetDate(details['expiry']))
  return contract_details


def GetPrices(contract_details, start_date, num_days):
  # Get the index prices and all futures prices
  index_prices = GetBTCDailyPrices(INDEX_SYMBOL, start_date, num_days)
  futures_prices = {}
  for contract, details in contract_details.items():
    if start_date > details.listing:
      continue
    start_date = max(start_date, details.listing)
    days_to_expiry = (details.expiry - start_date).days
    if days_to_expiry > 0:
      # TODO: Adjust start_date to start date of contract.
      futures_prices[contract] = GetBTCDailyPrices(
          contract, start_date, days_to_expiry)
  return index_prices, futures_prices


def GetBasisRates(contract_details, futures_prices, index_prices):
  # Get the daily basis for each future
  futures_basis = {}
  for contract, prices in futures_prices.items():
    futures_basis[contract] = GetDailyBasis(
        contract_details[contract].expiry, prices, index_prices)
  return futures_basis


if __name__ == '__main__':
  num_days = 365 * 4
  end_date = datetime.date.today() - datetime.timedelta(days=1)
  start_date = end_date - datetime.timedelta(days=num_days)
  contract_details = GetContractDetails(start_date, num_days)
  index_prices, futures_prices = GetPrices(contract_details,
                                           start_date, num_days)
  futures_basis = GetBasisRates(contract_details, futures_prices,
                                index_prices)
  daily_funding = GetDailyFunding('XBTUSD', start_date, num_days)
  for contract, basis in futures_basis.items():
    print(contract)
    print(basis)
    print(futures_prices[contract])
  print(daily_funding)
