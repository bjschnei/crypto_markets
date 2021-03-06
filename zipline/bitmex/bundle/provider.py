import asyncio
import datetime
import enum
import gzip
import io
import os
import pytz
import requests
import tempfile
import urllib

import pandas as pd
import requests_html

class BitmexDataProvider(object):

  class Granularity(enum.Enum):
    DAY = 1
    HOUR = 2
    MINUTE = 3

  def __init__(self, start_session, end_session):
    self._start_session = start_session
    self._end_session = end_session
    self._session = requests_html.AsyncHTMLSession()
    self._trade_file_urls = None
    self._cached_asset_details = {}

  async def Close(self):
    """Closes the underlying requests_html session."""
    await self._session.close()

  async def LoadData(self, granularity=Granularity.HOUR):
    """Generates Open/High/Low/Close/Volume for all Bitmex assets.

    Args:
      granularity: Duration of each candle
    Returns:
      Multi-Index DataFrame for each asset based on granularity.
      The first level of the multi-index is the symbol.
    """
    async for df in self._ConvertFilesToDataFrames():
      df['timestamp'] = pd.to_datetime(df['timestamp'],
                                       format='%Y-%m-%dD%H:%M:%S.%f')
      df['date'] = df['timestamp'].dt.date
      df['hour'] = df['timestamp'].dt.hour
      df['minute'] = df['timestamp'].dt.minute
      if granularity is self.Granularity.DAY:
        group = df.groupby(['symbol', 'date'])
      elif granularity is self.Granularity.HOUR:
        group = df.groupby(['symbol', 'date', 'hour'])
      else:  # MINUTE
        group = df.groupby(['symbol', 'date', 'hour', 'minute'])
      ohlcv = group.agg(
          {'price': ['first', 'max', 'min', 'last',], 'size': 'sum'})
      yield ohlcv

  async def GetAssetDetails(self, ohlcv):
    """Fetch all AssetDetails for each asset in the ohlcv data.

    Uses the Bitmex API to lookup a contract:
      https://www.bitmex.com/api/v1/instrument?symbol=<symbol>

    Args:
      ohlcv: DataFrame returned by LoadData
    """
    results = {}
    symbols = set(ohlcv.index.get_level_values(level=0))
    base_url = 'https://www.bitmex.com/api/v1/instrument?symbol='
    for symbol in symbols:
      details = self._cached_asset_details.get(symbol)
      if details is None:
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None, requests.get, base_url + symbol)
        self._cached_asset_details[symbol] = response.json()[0]
      results[symbol] = self._cached_asset_details[symbol]
    return results

  async def _ConvertFilesToDataFrames(self):
    for url in await self.GetTradeFileUrls():
      with urllib.request.urlopen(url) as response:
        yield pd.read_csv(io.BytesIO(gzip.decompress(response.read())))

  async def GetTradeFileUrls(self):
    """Return value is cached."""
    if self._trade_file_urls is not None:
      return self._trade_file_urls

    # Find the trade link, just in case it changes
    for sleep_secs in range(20):
      r = await self._session.get('https://public.bitmex.com')
      await r.html.arender(sleep=sleep_secs)
      listing = r.html.find('#listing', first=True)
      trade_link = None
      for link in listing.links:
        if 'trade' in link:
          trade_link = link
          break
      if trade_link is not None:
        break
    if trade_link is None:
      raise Exception('Trade link not found')

    # Get all the files gzip trade files
    # Sometimes it takes a while for the links to render.
    for sleep_secs in range(20):
      r = await self._session.get(trade_link)
      await r.html.arender(sleep=sleep_secs)
      listing = r.html.find('#listing', first=True)
      if listing.links:
        break
    if not listing or not listing.links:
      raise Exception("Urls to download not found")
    filtered_links = []
    for link in listing.links:
      if not link.endswith('.csv.gz'):
        continue
      # date in filename is format YYYYMMDD
      link_date = link[link.rfind('/') + 1 :link.rfind('.csv')]
      link_datetime = pytz.utc.localize(
          datetime.datetime.strptime(link_date, '%Y%m%d'))
      if (link_datetime >= self._start_session and
          link_datetime <= self._end_session):
        filtered_links.append(link)
    self._trade_file_urls = sorted(filtered_links)
    return self._trade_file_urls


async def main():
  bmdp = BitmexDataProvider(
      pytz.utc.localize(datetime.datetime.now()) - datetime.timedelta(days=4),
      pytz.utc.localize(datetime.datetime.now()))
  async for ohlcv in bmdp.LoadData():
    print(ohlcv)
    print(await bmdp.GetAssetDetails(ohlcv))
  await bmdp.Close()


if __name__ == '__main__':
  event_loop = asyncio.get_event_loop()
  try:
    event_loop.run_until_complete(main())
  finally:
    pending_tasks = [
      task for task in asyncio.Task.all_tasks() if not task.done()]
    event_loop.run_until_complete(asyncio.gather(*pending_tasks))
    event_loop.close()