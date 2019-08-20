import asyncio
import datetime
import pytz

import pandas as pd
from zipline.data import bundles

from bundle import converter

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
  event_loop = asyncio.get_event_loop()
  try:
    event_loop.run_until_complete(
      converter.LoadData(asset_db_writer, daily_bar_writer, show_progress,
                         start_session, end_session))
  finally:
    pending_tasks = [
      task for task in asyncio.Task.all_tasks() if not task.done()]
    event_loop.run_until_complete(asyncio.gather(*pending_tasks))
    event_loop.close()

  # create empty SQLite tables to prevent lookup errors in algorithms
  divs_splits = {
      'divs': pd.DataFrame(columns=['sid', 'amount', 'ex_date', 'record_date',
                                    'declared_date', 'pay_date']),
      'splits': pd.DataFrame(columns=['sid', 'ratio', 'effective_date'])}
  adjustment_writer.write(
      splits=divs_splits['splits'], dividends=divs_splits['divs'])


bundles.register(
    'bitmex',
    ingest,
    start_session=pd.Timestamp(pytz.utc.localize(datetime.datetime(2019, 8, 9))),
    end_session=pd.Timestamp(pytz.utc.localize(datetime.datetime(2019, 8, 9))),
)
