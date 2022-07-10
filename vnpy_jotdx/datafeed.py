from datetime import datetime
from typing import List, Optional

import pandas as pd

from jotdx.exhq import TdxExHq_API
from jotdx.hq import TdxHq_API
from jotdx.utils import to_data
from jotdx.utils.best_ip_async import select_best_ip_async

from joconst.maps import INTERVAL_TDX_MAP, JONPY_TDX_MARKET_MAP
from joconst.constant import Exchange, Interval, TdxMarket, TdxCategory
from joconst.object import BarData, TickData, HistoryRequest
from tqdm import tqdm

from vnpy.trader.datafeed import BaseDatafeed


def trans_datetime_range_to_start_count(start: datetime, end: datetime, interval: Interval, market_flag="ext"):
    now = datetime.now().replace(tzinfo=end.tzinfo)
    if end > now:
        end = now

    datetime_range_days = (end - start).days
    now_to_end_days = max((now - end).days, 0)
    count = datetime_range_days if datetime_range_days < 7 else (datetime_range_days * 5 / 7)
    count_offset = now_to_end_days if now_to_end_days < 7 else (now_to_end_days * 5 / 7)

    # 21: 00 - 次日2:30, 共 5.5 小时
    # 上午：09: 00 - 10:15 10: 30 - 11:30, 共 2.5 小时
    # 下午：13: 30 - 15:00, 共 1.5 小时
    # 一共 9.5 小时
    HOUR_PER_DAY_MAX = 9.5 if market_flag == "ext" else 4.5
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


def gen_concat_data(total_count, category, market, code, start, get_bars_func):
    COUNT_MAX = 700

    if total_count <= COUNT_MAX:
        data = get_bars_func(
            category=category, market=market, code=code,
            start=start, count=total_count
        )
    else:
        data = []
        while total_count > COUNT_MAX:
            origin_data_len = len(data)

            data = get_bars_func(
                category=category, market=market, code=code,
                start=start, count=COUNT_MAX
            ) + data

            if origin_data_len == len(data):
                total_count = 0
                break

            total_count -= COUNT_MAX
            start += COUNT_MAX

        if total_count > 0:
            data = get_bars_func(
                category=category, market=market, code=code,
                start=start, count=total_count
            ) + data

    return data


class JotdxDatafeed(BaseDatafeed):
    """JotdxData数据服务接口"""

    def __init__(self):
        """"""
        self.inited: bool = False
        self.ext_api = None
        self.std_api = None
        self.sh_index_daily_bar_df = None

        self.future_market_category_list = [
            (TdxMarket.DCE, TdxCategory.DCE),
            (TdxMarket.SHFE, TdxCategory.SHFE),
            (TdxMarket.CZCE, TdxCategory.CZCE),
            (TdxMarket.CFFEX, TdxCategory.CFFEX),

            (TdxMarket.SGE, TdxCategory.SGE),
            (TdxMarket.HKSE, TdxCategory.HKSE),
        ]

        self.future_market_list = [
            TdxMarket.DCE, TdxMarket.SHFE, TdxMarket.CZCE, TdxMarket.CFFEX,
            TdxMarket.SGE, TdxMarket.HKSE
        ]
        self.stock_market_list = [TdxMarket.SSE, TdxMarket.SZSE, TdxMarket.BSE]

    def init(self) -> bool:
        """初始化"""
        if self.inited:
            return True

        future_best_ip_port_dict = select_best_ip_async(_type='future')
        self.ext_api = TdxExHq_API(heartbeat=True)
        self.ext_api.connect(ip=future_best_ip_port_dict['ip'], port=future_best_ip_port_dict['port'])

        stock_best_ip_port_dict = select_best_ip_async(_type='stock')
        self.std_api = TdxHq_API(heartbeat=True)
        self.std_api.connect(ip=stock_best_ip_port_dict['ip'], port=stock_best_ip_port_dict['port'])

        # 记录上证指数 日k线 df, 作为交易日历
        self.sh_index_daily_bar_df = self.get_sh_index_daily_bar_df()

        # self.markets = self.ext_api.to_df(self.ext_api.get_markets())
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
        # market = self.markets[self.markets['name'] == EXCHANGE_NAME_MAP[req.exchange]]["market"].iat[0]

        market = JONPY_TDX_MARKET_MAP[req.exchange]
        start = req.start
        end = req.end

        get_bar_data_func = None

        if market in self.future_market_list:
            count_offset, count = trans_datetime_range_to_start_count(start, end, req.interval, market_flag="ext")
            get_bar_data_func = self.ext_api.get_instrument_bar_data
        elif market in self.stock_market_list:
            count_offset, count = trans_datetime_range_to_start_count(start, end, req.interval, market_flag="std")
            get_bar_data_func = self.std_api.get_security_bar_data
        else:
            raise ValueError(f"{market} is not TdxMarket attribute value.")

        data = gen_concat_data(
            total_count=count, category=category, market=market, code=code,
            start=count_offset,
            get_bars_func=get_bar_data_func
        )

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
        market = JONPY_TDX_MARKET_MAP[req.exchange]
        start = req.start
        end = req.end

        market_flag = ""
        get_bars_func = None

        if market in self.future_market_list:
            market_flag = "ext"
            count_offset, count = trans_datetime_range_to_start_count(start, end, req.interval, market_flag=market_flag)
            get_bars_func = self.ext_api.get_instrument_bars
        elif market in self.stock_market_list:
            market_flag = "std"
            count_offset, count = trans_datetime_range_to_start_count(start, end, req.interval, market_flag=market_flag)
            get_bars_func = self.std_api.get_security_bars
        else:
            raise ValueError(f"{market} is not TdxMarket attribute value.")

        data = gen_concat_data(
            total_count=count, category=category, market=market, code=code,
            start=count_offset,
            get_bars_func=get_bars_func
        )

        return self.to_df(data, market_flag=market_flag)

    def get_sh_index_daily_bar_df(self):

        all_df = pd.DataFrame()
        frequency = INTERVAL_TDX_MAP[Interval.DAILY]
        # 这里不能用 util 里面的函数去根据code判断market, 因为指数是000001,会被误判为深圳
        market = TdxMarket.SSE
        start = 0
        offset = 700

        while True:
            temp_df = self.std_api.to_df(
                self.std_api.get_index_bars(
                    category=frequency, market=market, code="000001", start=start, count=offset
                )
            )

            if temp_df.empty:
                break

            all_df = pd.concat([temp_df, all_df])
            start += offset

        return all_df

    def to_df(self, data, market_flag="ext"):
        if market_flag == "ext":
            data_df: pd.DataFrame = self.ext_api.to_df(data)
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

        elif market_flag == "std":
            data_df: pd.DataFrame = self.std_api.to_df(data)
            data_df.rename(
                columns={
                    "open": "open_price",
                    "high": "high_price",
                    "low": "low_price",
                    "close": "close_price",
                    "vol": "volume",
                    "amount": "turnover",
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

        if market in self.future_market_list:

            while True:
                r = self.ext_api.to_df(self.ext_api.get_instrument_quote_list(market, category, start=start))
                start += len(r)
                if len(r) == 0:
                    break
                contract_df = pd.concat([contract_df, r])

        elif market in self.stock_market_list:

            counts = self.std_api.get_security_count(market=market)

            for start in tqdm(range(0, counts, 1000)):
                result = self.std_api.get_security_list(market=market, start=start)
                contract_df = (
                    pd.concat([contract_df, to_data(result)], ignore_index=True) if start > 1 else to_data(result)
                )

        return contract_df

    def query_all_contracts_df(self):
        '''
        TODO 暂时只返回所有期货合约的 df
        '''
        all_contracts_df = pd.DataFrame()
        for market, category in self.future_market_category_list:
            if market not in [TdxMarket.SGE, TdxMarket.HKSE]:
                all_contracts_df = pd.concat([all_contracts_df, self.query_contract_df(market, category)])
        return all_contracts_df

    def close(self):
        self.ext_api.close()
        self.std_api.close()


if __name__ == '__main__':
    from datetime import datetime

    bar_req1 = HistoryRequest(
        symbol="CUL8",
        exchange=Exchange("SHFE"),
        start=datetime(2022, 6, 14),
        end=datetime.now(),
        interval=Interval.MINUTE
    )

    bar_req2 = HistoryRequest(
        symbol="000001",
        exchange=Exchange("SSE"),
        start=datetime(1990, 1, 1),
        end=datetime.now(),
        interval=Interval.DAILY
    )

    bar_req3 = HistoryRequest(
        symbol="300123",
        exchange=Exchange("SZSE"),
        start=datetime(2022, 6, 14),
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
    # cul8_data = datafeed.query_bar_history(bar_req1)
    # cul8_data_df = datafeed.query_bar_df_history(bar_req1)
    #
    sse_data = datafeed.query_bar_history(bar_req2)
    sse_data_df = datafeed.query_bar_df_history(bar_req2)
    #
    # szse_data = datafeed.query_bar_history(bar_req3)
    # szse_data_df = datafeed.query_bar_df_history(bar_req3)

    # 获取合约数据
    dce_con_df = datafeed.query_contract_df(market=TdxMarket.DCE, category=TdxCategory.DCE)
    shfe_con_df = datafeed.query_contract_df(market=TdxMarket.SHFE, category=TdxCategory.SHFE)
    czce_con_df = datafeed.query_contract_df(market=TdxMarket.CZCE, category=TdxCategory.CZCE)
    cffex_con_df = datafeed.query_contract_df(market=TdxMarket.CFFEX, category=TdxCategory.CFFEX)

    hkse_con_df = datafeed.query_contract_df(market=TdxMarket.HKSE, category=TdxCategory.HKSE)
    sge_con_df = datafeed.query_contract_df(market=TdxMarket.SGE, category=TdxCategory.SGE)

    sse_con_df = datafeed.query_contract_df(market=TdxMarket.SSE)
    szse_con_df = datafeed.query_contract_df(market=TdxMarket.SZSE)
    bse_con_df = datafeed.query_contract_df(market=TdxMarket.BSE)

    df = datafeed.query_all_contracts_df()
    for idx, ss in df[df['code'].str.contains('L8')][['market', 'code']].iterrows():
        market1 = ss['market']
        code1 = ss['code']
        print(1)
    print(1)

    # 获取tick历史数据
    # data = datafeed.query_tick_history(tick_req)
