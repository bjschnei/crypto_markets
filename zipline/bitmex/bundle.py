import requests_html

def GetTradeFileUrls():
  session = requests_html.HTMLSession()

  # Find the trade link
  r = session.get('https://public.bitmex.com')
  r.html.render(sleep=1)
  listing = r.html.find('#listing', first=True)
  trade_link = None
  for link in listing.links:
    if 'trade' in link:
      trade_link = link
      break
  if trade_link is None:
    raise Exception('Trade link not found')

  r = session.get(trade_link)
  r.html.render(sleep=1)
  listing = r.html.find('#listing', first=True)
  return list(listing.links)

if __name__ == '__main__':
  print(GetTradeFileUrls())
