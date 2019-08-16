import asyncio
import collections
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
  #print('*' * 80)
  #print(asset_details)
  if asset_details['expiry'] is None:
    return None
  # print(asset_details)
  expiry_date = ConvertBitmexDateTime(asset_details['expiry'])
  return {
    'exchange': 'BITMEX',
    'symbol': asset_details['symbol'],
    'root_symbol': asset_details['rootSymbol'],
    'asset_name': asset_details['underlying'],
    'notice_date': ConvertBitmexDateTime(asset_details['listing']),
    'expiration_date': expiry_date,
    'auto_close_date': expiry_date + datetime.timedelta(days=1),
    'tick_size': asset_details['tickSize'],
    'multiplier': asset_details['multiplier']
  }

async def LoadData(asset_db_writer, daily_bar_writer, show_progress,
                   start_session, end_session):
  bmdp = provider.BitmexDataProvider(start_session, end_session)
  futures_df = pd.DataFrame()
  async for ohlcv in bmdp.LoadData():
    new_details_df = pd.DataFrame()
    asset_details = await bmdp.GetAssetDetails(ohlcv)
    for asset_detail in asset_details.values():
      detail_data = GetFutureNeededAssetDetails(asset_detail)
      if detail_data is not None:
        # futures sid is the futures_df.index
        new_details_df = new_details_df.append(
          pd.DataFrame(pd.Series(detail_data)).T, ignore_index=True)
    futures_df = (
        futures_df.append(new_details_df)
        .drop_duplicates()
        .rename_axis('sid')
    )
    print(ohlcv)
    print(futures_df)
    # TODO: join the ohlcv data with the sid of the asset.
    # Write out the daily bar data.
    # t = ohlcv.merge_join(futures_df, left_on='symbol', right_on='symbol')
    # print(t)
    #daily_bar_writer.write(show_progress=show_progress)

  root_symbols_df = futures_df[['root_symbol', 'exchange']].drop_duplicates()
  root_symbols_df['root_symbol_id'] = root_symbols_df['root_symbol'].apply(hash)
  asset_db_writer.write(futures=futures_df,
                        root_symbols=root_symbols_df)
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
      LoadData(asset_db_writer, daily_bar_writer, show_progress, start_session,
               end_session))
  finally:
    pending_tasks = [
      task for task in asyncio.Task.all_tasks() if not task.done()]
    event_loop.run_until_complete(asyncio.gather(*pending_tasks))
    event_loop.close()


bundles.register('bitmex', ingest)