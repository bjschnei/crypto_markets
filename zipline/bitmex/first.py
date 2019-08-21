from zipline.api import order, record, symbol, future_symbol
from zipline.finance.commission import PerShare, PerTrade

import bundle


def initialize(context):
  context.set_commission( us_equities=PerTrade(0), us_futures=PerTrade(0), )


def handle_data(context, data):
  s = 'XBTU19'
  order(future_symbol(s), 10)
  #record(XBT=data.current(future_symbol(s), 'price'))

def before_trading_start(context, data):
  """
  print(data)
  print(dir(data))
  print(data.items())
  """



