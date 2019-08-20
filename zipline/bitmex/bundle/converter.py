import datetime
import pytz

import pandas as pd
from zipline.utils import cli

from bundle import provider

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


def GetOHLCVPerSid(ohlcv_df, futures_df):
  """Produce data that can be passed to the bar writter.

  Args:
    ohlcv_df: OHLCV data returned by BitmexDataProvider.LoadData
    futures_df:  Asset details returned by GetFutureNeededAssetDetails

  Returns:
    list if pairs of sid, ohlcv dataframes
    dataframe is indexed by timestamp.
    See format expected by: http://www.zipline.io/appendix.html#writers
  """
  # TODO ohlcv price data must be 1000x

  # Join the ohlcv data with the sid of the asset.
  ohlcv_df.columns = ohlcv_df.columns.droplevel()
  futures_df['sid'] = futures_df.index
  price_data_df = ohlcv_df.reset_index().merge(futures_df, on='symbol')
  price_data_df.rename(inplace=True, columns={
      'first': 'open',
      'last': 'close',
      'max': 'high',
      'min': 'low',
      'sum': 'volume'})
  sid_prices = price_data_df.groupby('sid')
  return [
      (sid,
       df[['date', 'open', 'high', 'low', 'close', 'volume']]
           .set_index('date')
      )
      for sid,df in sid_prices
  ]


async def LoadData(asset_db_writer, daily_bar_writer, show_progress,
                   start_session, end_session):
  bmdp = provider.BitmexDataProvider(start_session, end_session)
  futures_df = pd.DataFrame()
  urls = await bmdp.GetTradeFileUrls()
  with cli.maybe_show_progress(
      range(len(urls)),
      show_progress=show_progress,
      label="Loading BitMEX data",
      item_show_func=lambda x: urls[x] if x is not None else '') as progress:
    for _ in progress:
      # TODO: Pass granularity at command line.
      async for ohlcv in bmdp.LoadData(
          granularity=provider.BitmexDataProvider.Granularity.DAY):
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
            .rename_axis('sid')
            .drop_duplicates('asset_name')
        )

        daily_bar_writer.write(
            GetOHLCVPerSid(ohlcv, futures_df), show_progress=show_progress)

  root_symbols_df = futures_df[['root_symbol', 'exchange']].drop_duplicates()
  root_symbols_df['root_symbol_id'] = root_symbols_df['root_symbol'].apply(hash)
  asset_db_writer.write(futures=futures_df,
                        root_symbols=root_symbols_df)
  await bmdp.Close()
