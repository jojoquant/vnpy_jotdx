from datetime import datetime
from typing import List, Optional

import pandas as pd

from jotdx.exhq import TdxExHq_API
from jotdx.utils.best_ip_async import select_best_ip_async

from joconst.maps import EXCHANGE_NAME_MAP, INTERVAL_TDX_MAP
from joconst.constant import Exchange, Interval, TdxMarket, TdxCategory
from joconst.object import BarData, TickData, HistoryRequest

from vnpy.trader.datafeed import BaseDatafeed


def trans_datetime_range_to_start_count(start: datetime, end: datetime, interval: Interval):
    datetime_range_days = (end - start).days
    now = datetime.now().replace(tzinfo=end.tzinfo)
    now_to_end_days = (now - end).days
    count = datetime_range_days if datetime_range_days < 7 else (datetime_range_days * 5 / 7)
    count_offset = now_to_end_days if now_to_end_days < 7 else (now_to_end_days * 5 / 7)

    # 21: 00 - 次日2:30, 共 5.5 小时
    # 上午：09: 00 - 10:15 10: 30 - 11:30, 共 2.5 小时
    # 下午：13: 30 - 15:00, 共 1.5 小时
    # 一共 9.5 小时
    HOUR_PER_DAY_MAX = 9.5
    HOUR_PER_DAY_MIN = 4
    units = 1
    if interval == Interval.MINUTE:
        units = 60
    elif interval == Interval.MINUTE_5:
        units = 12
    elif interval == Interval.MINUTE_15:
        units = 4
    elif interval == Interval.MINUTE_30:
        units = 2
    elif interval == Interval.HOUR:
        units = 1
    elif interval == Interval.DAILY:
        HOUR_PER_DAY_MAX = 1
        HOUR_PER_DAY_MIN = 1
        units = 1
    elif interval == Interval.WEEKLY:
        HOUR_PER_DAY_MAX = 1
        HOUR_PER_DAY_MIN = 1
        units = 0.2  # 1/5

    count = int(count * HOUR_PER_DAY_MAX * units)
    count_offset = int(count_offset * HOUR_PER_DAY_MIN * units)
    return count_offset, count


class JotdxDatafeed(BaseDatafeed):
    """JotdxData数据服务接口"""

    def __init__(self):
        """"""
        self.inited: bool = False
        self.api = None
        self.markets: pd.DataFrame = None
        self.future_market_category_list = [
            (TdxMarket.DCE, TdxCategory.DCE),
            (TdxMarket.SHFE, TdxCategory.SHFE),
            (TdxMarket.CZCE, TdxCategory.CZCE),
            (TdxMarket.CFFEX, TdxCategory.CFFEX),
        ]

    def init(self) -> bool:
        """初始化"""
        if self.inited:
            return True

        future_best_ip_port_dict = select_best_ip_async(_type='future')
        # stock_best_ip_port_dict = select_best_ip_async(_type='stock')

        self.api = TdxExHq_API(heartbeat=True)
        self.api.connect(ip=future_best_ip_port_dict['ip'], port=future_best_ip_port_dict['port'])
        self.markets = self.api.to_df(self.api.get_markets())
        self.inited = True
        return True

    def query_bar_history(self, req: HistoryRequest) -> Optional[List[BarData]]:
        """
        查询K线数据
        start 和 end 时间定位是不精确的
        """
        if not self.inited:
            self.init()

        code = req.symbol
        category = INTERVAL_TDX_MAP[req.interval]
        market = self.markets[self.markets['name'] == EXCHANGE_NAME_MAP[req.exchange]]["market"].iat[0]
        start = req.start
        end = req.end

        count_offset, count = trans_datetime_range_to_start_count(start, end, req.interval)
        COUNT_MAX = 700

        if count <= COUNT_MAX:
            data = self.api.get_instrument_bar_data(
                category=category, market=market, code=code,
                start=count_offset, count=count
            )
        else:
            data = []
            while count > COUNT_MAX:
                data = self.api.get_instrument_bar_data(
                    category=category, market=market, code=code,
                    start=count_offset, count=COUNT_MAX
                ) + data
                count -= COUNT_MAX
                count_offset += COUNT_MAX

            if count > 0:
                data = self.api.get_instrument_bar_data(
                    category=category, market=market, code=code,
                    start=count_offset, count=count
                ) + data

        return data

    def query_bar_df_history(self, req: HistoryRequest) -> pd.DataFrame:
        """
        查询K线数据
        start 和 end 时间定位是不精确的
        """
        if not self.inited:
            self.init()

        code = req.symbol
        category = INTERVAL_TDX_MAP[req.interval]
        market = self.markets[self.markets['name'] == EXCHANGE_NAME_MAP[req.exchange]]["market"].iat[0]
        start = req.start
        end = req.end

        count_offset, count = trans_datetime_range_to_start_count(start, end, req.interval)
        COUNT_MAX = 700

        if count <= COUNT_MAX:
            data = self.api.get_instrument_bars(
                category=category, market=market, code=code,
                start=count_offset, count=count
            )
        else:
            data = []
            while count > COUNT_MAX:
                data = self.api.get_instrument_bars(
                    category=category, market=market, code=code,
                    start=count_offset, count=COUNT_MAX
                ) + data
                count -= COUNT_MAX
                count_offset += COUNT_MAX

            if count > 0:
                data = self.api.get_instrument_bars(
                    category=category, market=market, code=code,
                    start=count_offset, count=count
                ) + data

        data_df: pd.DataFrame = self.api.to_df(data)
        data_df.rename(
            columns={
                "open": "open_price",
                "high": "high_price",
                "low": "low_price",
                "close": "close_price",
                "trade": "volume",
                "position": "open_interest",
            }, inplace=True
        )
        return data_df

    # TODO
    def query_tick_history(self, req: HistoryRequest) -> Optional[List[TickData]]:
        """查询Tick数据"""
        if not self.inited:
            self.init()

        data: List[TickData] = []

        return data

    def query_contract_df(self, market=TdxMarket.DCE, category=TdxCategory.DCE):
        contract_df = pd.DataFrame()
        start = 0
        while True:
            r = self.api.to_df(self.api.get_instrument_quote_list(market, category, start=start))
            start += len(r)
            if len(r) == 0:
                break
            contract_df = pd.concat([contract_df, r])
        return contract_df

    def query_all_contracts_df(self):
        all_contracts_df = pd.DataFrame()
        for market, category in self.future_market_category_list:
            all_contracts_df = pd.concat([all_contracts_df, self.query_contract_df(market, category)])
        return all_contracts_df

    def close(self):
        self.api.disconnect()


if __name__ == '__main__':
    from datetime import datetime

    bar_req = HistoryRequest(
        symbol="CUL8",
        exchange=Exchange("SHFE"),
        start=datetime(2022, 3, 4),
        end=datetime.now(),
        interval=Interval.MINUTE
    )

    tick_req = HistoryRequest(
        symbol="CU888",
        exchange=Exchange("SHFE"),
        start=datetime(2022, 3, 8),
        end=datetime.now(),
        interval=Interval.TICK
    )

    # 获取数据服务实例
    datafeed = JotdxDatafeed()
    datafeed.init()

    # 获取k线历史数据
    # data = datafeed.query_bar_history(bar_req)
    # data_df = datafeed.query_bar_df_history(bar_req)

    # 获取合约数据
    # datafeed.query_contract_df()
    df = datafeed.query_all_contracts_df()
    for idx, ss in df[df['code'].str.contains('L8')][['market', 'code']].iterrows():
        market1 = ss['market']
        code1 = ss['code']
        print(1)
    print(1)

    # 获取tick历史数据
    # data = datafeed.query_tick_history(tick_req)
