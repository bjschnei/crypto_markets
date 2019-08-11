import asyncio
import datetime
import pytz

import pandas as pd
from zipline.data import bundles

import provider

DEFAULT_NUM_DAYS = 5

def ConvertBitmexDateTime(date_time_str):
  """Converts string to datetime.datetime.

  Format is like 2019-10-25T12:00:00.000Z
  """
  return pytz.utc.localize(
      datetime.datetime.strptime(
          date_time_str, '%Y-%m-%dT%H:%M:%S.%fZ'))

def GetFutureNeededAssetDetails(asset_details):
  """Trim asset details provided to just what zipline needs.

  See: https://www.zipline.io/appendix.html#zipline.assets.AssetDBWriter

  Returns:
    None if the Asset isn't a future
  """
  if asset_details['expiry'] is None:
    return None
  symbol = asset_details['symbol']
  root_symbol = asset_details['rootSymbol']
  asset_name = asset_details['underlying']

  exchange = 'BITMEX'
  expiration_date = ConvertBitmexDateTime(asset_details['expiry'])
  auto_close_date = expiration_date
  tick_size = asset_details['tickSize']
  multiplier = asset_details['multiplier']

async def LoadData(asset_db_writer, start_session, end_session):
  bmdp = provider.BitmexDataProvider(start_session, end_session)
  async for ohlcv in bmdp.LoadData():
    # print(ohlcv)
    print('*' * 80)
    asset_details = await bmdp.GetAssetDetails(ohlcv)
    for k,v in asset_details.items():
      print('#'*80)
      print(k)
      print(v)
      GetFutureNeededAssetDetails(v)
    asset_db_writer.write(futures=None)
  await bmdp.Close()


def ingest(environ,
           asset_db_writer,
           minute_bar_writer,
           daily_bar_writer,
           adjustment_writer,
           calendar,
           start_session,
           end_session,
           cache,
           show_progress,
           output_dir):
  # hard code for just 1 day for now
  start_session = pd.Timestamp(datetime.date(2019, 8, 9))
  end_session = pd.Timestamp(datetime.date(2019, 8, 9))

  event_loop = asyncio.get_event_loop()
  try:
    event_loop.run_until_complete(
      LoadData(asset_db_writer, start_session, end_session))
  finally:
    pending_tasks = [
      task for task in asyncio.Task.all_tasks() if not task.done()]
    event_loop.run_until_complete(asyncio.gather(*pending_tasks))
    event_loop.close()


bundles.register('bitmex', ingest)