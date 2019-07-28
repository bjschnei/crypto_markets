import asyncio
import datetime
import enum
import gzip
import io
import os
import tempfile
import urllib
import shutil

import pandas as pd
import requests_html

class BitmexDataProvider(object):

  class Granularity(enum.Enum):
    DAY = 1
    HOUR = 2
    MINUTE = 3

  def __init__(self, num_days):
    self._num_days = num_days
    self._session = requests_html.AsyncHTMLSession()

  async def Close(self):
    await self._session.close()

  async def LoadData(self, granularity=Granularity.HOUR):
    async for df in self._ConvertFilesToDataFrames():
      df['timestamp'] = pd.to_datetime(df['timestamp'],
                                       format='%Y-%m-%dD%H:%M:%S.%f')
      df['date'] = df['timestamp'].map(lambda x:x.date())
      df['hour'] = df['timestamp'].map(lambda x:x.hour)
      df['minute'] = df['timestamp'].map(lambda x:x.minute)
      if granularity is self.Granularity.DAY:
        group = df.groupby(['symbol', 'date'])
      elif granularity is self.Granularity.HOUR:
        group = df.groupby(['symbol', 'date', 'hour'])
      else:  # MINUTE
        group = df.groupby(['symbol', 'date', 'hour', 'minute'])
      ohlcv = group.agg(
          {'price': ['first', 'max', 'min', 'last',], 'size': 'sum'})
      yield ohlcv

  async def _ConvertFilesToDataFrames(self):
    for url in await self._GetTradeFileUrls():
      with urllib.request.urlopen(url) as response:
        yield pd.read_csv(io.BytesIO(gzip.decompress(response.read())))

  async def _GetTradeFileUrls(self):
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
    links = sorted(link for link in listing.links if link.endswith('gz'))
    return links[self._num_days * -1:]


async def main():
  bmdp = BitmexDataProvider(2)
  async for data in bmdp.LoadData():
    print(data)
  await bmdp.Close()


if __name__ == '__main__':
  event_loop = asyncio.get_event_loop()
  try:
    event_loop.run_until_complete(main())
    pending_tasks = [
      task for task in asyncio.Task.all_tasks() if not task.done()]
    event_loop.run_until_complete(asyncio.gather(*pending_tasks))
  finally:
    event_loop.close()