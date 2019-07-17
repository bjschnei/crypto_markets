import asyncio
import gzip
import io
import os
import tempfile
import urllib
import shutil

import pandas
import requests_html

class BitmexDataProvider(object):

  def __init__(self, num_days):
    self._num_days = num_days
    self._session = requests_html.AsyncHTMLSession()

  async def Close(self):
    await self._session.close()

  async def LoadData(self):
    async for frame in self._ConvertFilesToDataFrames():
      return frame

  async def _ConvertFilesToDataFrames(self):
    for url in await self._GetTradeFileUrls():
      with urllib.request.urlopen(url) as response:
        yield pandas.read_csv(io.BytesIO(gzip.decompress(response.read())))

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
  bmdp = BitmexDataProvider(10)
  print(await bmdp.LoadData())
  await bmdp.Close()


if __name__ == '__main__':
  event_loop = asyncio.get_event_loop()
  try:
    event_loop.run_until_complete(main())
  finally:
    event_loop.close()