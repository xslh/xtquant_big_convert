# run.py
from bigqmt_signal_trader.xtquant_compat import StockAccount, configure, xt_trader, xtdata

configure()

acc = StockAccount(xt_trader.client.account_id, "STOCK")

# 行情
ticks = xtdata.get_full_tick(["000001.SZ"])
print(ticks["000001.SZ"]["lastPrice"])

# 持仓 / 资金
positions = xt_trader.query_stock_positions(acc)
asset = xt_trader.query_stock_asset(acc)
print(asset.cash, asset.total_asset)

# K线（自动还原成 pandas DataFrame）
klines = xtdata.get_market_data_ex(
    field_list=["close"], stock_list=["000001.SZ"], period="1d", count=5
)
