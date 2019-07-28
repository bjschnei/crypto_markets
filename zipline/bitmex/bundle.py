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
  bitmex_provider = provider.BitmexDataProvider(start_session, end_session)
  print(output_dir)


bundles.register('bitmex', ingest)