from zipline.data import bundles

import provider

DEFAULT_NUM_DAYS = 5

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
  num_days = environ.get('NUM_DAYS', DEFAULT_NUM_DAYS)
  bitmex_provider = provider.BitmexDataProvider(num_days)


bundles.register('bitmex', ingest)