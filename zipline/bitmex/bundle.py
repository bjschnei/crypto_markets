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
  print(start_session)
  print(end_session)
  print(type(start_session))
  # TODO: BitmexDataProvider needs to take start/end as args
  # bitmex_provider = provider.BitmexDataProvider(1)


bundles.register('bitmex', ingest)