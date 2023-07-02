from datetime import datetime
import logging
from collections import defaultdict
import ujson as json
from database.redis_db import BaseDb
from multiprocessing import Process, Queue
from config.settings import DEBUG
from config.trading_map import Mapping
from common.msg import MessageHandle
from common.decorator import except_console
from common.calc import get_unixtime, trendline_trigger_calc, round_coin, change_unixtime
from time import sleep


class WatchDog():
    """
    GetPriceProc, NewOrderProc, TimeWatcher, PriceWatcher, LineWatcher 를 multiprocess 로 실행하고,
    종료시 다시 사작하게 process를 관리하는 class
    """
    def __init__(self):
        self.price_q = Queue() # price 정보를 담을 queue 생성
        self.get_price = GetPriceProc()
        self.new_order_proc = NewOrderProc()
        self.time_proc = TimeWatcher()
        self.price_watcher = PriceWatcher()
        self.line_watcher = LineWatcher()
        self.process_list = {'get_price': {'target': self.get_price.run, 'Q': [self.price_q]},
                            'new_order_proc': {'target': self.new_order_proc.run, 'Q': []},
                            'time_proc': {'target': self.time_proc.run, 'Q': []},
                            'price_watcher': {'target': self.price_watcher.run, 'Q': [self.price_q]},
                            'line_watcher': {'target': self.line_watcher.run, 'Q': []},
                            }

    def run(self):
        print('WatchDog Start!')
        ps_list =  list(self.process_list.keys())
        for _name in ps_list:
            self.process_list[_name]['proc'] = Process(target=self.process_list[_name]['target'], args=tuple(self.process_list[_name]['Q']))
            print('{} strt!'.format(_name))
            self.process_list[_name]['proc'].start()

        while True:
            try:
                for _name in ps_list:
                    if not self.process_list[_name]['proc'].is_alive():
                        print('{} terminate!'.format(_name))
                        self.process_list[_name]['proc'].join()
                        self.process_list[_name]['proc'] = Process(target=self.process_list[_name]['target'],
                                                                   args=tuple(self.process_list[_name]['Q']))
                        self.process_list[_name]['proc'].start()
                        print('{} start!'.format(_name))

            except Exception as e:
                print('RUN_LOOP_ERROR: ' + str(e))
            sleep(3)


class GetPriceProc(BaseDb, MessageHandle):
    """
    신규 price를 받아 price_queue에 전달
    """
    def __init__(self):
        super().__init__()
        self.price_queue = None

    def run(self, price_q):
        self.price_queue = price_q
        while True:
            self.get_price()

    @except_console
    def get_price(self):
        price = self.db_get_price()
        self.price_queue.put_nowait(price)


class NewOrderProc(BaseDb):
    """
    new 또는 change order 가 발생하면 해당주문정보를 watcher에 등록
    """
    def __init__(self):
        BaseDb.__init__(self)
        self.ticksize = self.get_ticksize()

    @except_console
    def run(self):
        """
        무한 loop를 통해 queue에 주문 id 수신대기
        :return:
        """
        all_list = self.db_order_list_getall()
        for _id, order in all_list.items():
            self.order_init(order)

        while True:
            # 신규 주문 수신
            self.new_order_handle()

    @except_console
    def new_order_handle(self):
        _id, order = self.db_get_order()
        res = self.set_order(_id, order)

    @except_console
    def order_init(self, order):
        order_info = json.loads(order)

        if order_info['planType'] == 'trendLine' and order_info['indicatorType'] == 'OPEN':
            self.set_trendline_queue(order_info)
        res = self.db_set_order(order_info)

    @except_console
    def set_order(self, _id, order):
        """
        new order가 들어오면 초기 실행
        :param _id:  '2as12-3asdfasdf-3-asdf-asdfaf'
        :param order: dict(order_detail 정보)
        :return: True / None
        """
        if DEBUG: print(str(datetime.now()), 'SET_ORDER: {}'.format(str(order)))

        if order['active'] not in Mapping.active_code:
            # active 가 아니면 remove
            if DEBUG: print(str(datetime.now()), 'IS_ACTIVE_FALSE: {}'.format(str(order)))
            self.remove_order(_id, order)
            return None

        if order['planType'] in ['reserved', 'horizontal', 'trendLine']:
            if order['status'] in ['WAITING', 'PENDING']:
                if order['indicatorType'] == 'TRAIL':
                    self.set_trail(_id, order)
                else:
                    if order['planType'] == 'trendLine':
                        if order['indicatorType'] == 'OPEN':
                            self.set_trendline(_id, order)
                        else:
                            self.set_reserved(_id, order)
                    else:
                        self.set_reserved(_id, order)

        elif order['planType'] == 'strategy':
            if (order['status'] in ['WAITING', 'PENDING']) and (order['indicatorType'] in ['OPEN', 'TAKE', 'LOSS']):
                self.set_strategy(_id, order)
            else:
                print('Unknown Type', order['planType'], order['status'])
        else:
            print('Unknown Type', order['planType'], order['status'])
        return True

    @except_console
    def remove_order(self, _id, order):
        if order['planType'] == 'strategy':
            order_info = self.db_get_order_info(_id)
            if order_info:
                if 'candleSize' in order_info['indicators'][0]:
                    _symbol_kline = '{}_{}'.format(order_info['symbol'], order_info['indicators'][0]['candleSize'])
                    res = self.set_indicator_order(_symbol_kline, _id, remove=True)

        self.rm_order( _id, order['symbol'], order['indicators'])

    @except_console
    def set_trail(self, _id, order):
        order_info = {}
        order_info['indicator'] = order['planType']
        if DEBUG: print(str(datetime.now()), 'SET_TRAIL: {}'.format(str(order)))
        for i in Mapping.item_root:
            order_info[i] = order[i]
        order_info['direction'] = -1 if order_info['side'] == 'BUY' else 1
        indicator = order['indicators'][0]
        if order['status'] == 'WAITING':
            order_info['price'] = indicator['triggerPrice']
        elif order['status'] == 'PENDING':
            print('Trailing status is pending', order)
            return
        else:
            print('Unknown status', order)
            return
        res = self.db_set_order(order_info)
        return res

    @except_console
    def set_reserved(self, _id, order):
        order_info = {}
        order_info['indicator'] = order['planType']
        if DEBUG: print(str(datetime.now()), 'SET_RESERVED: {}'.format(str(order)))
        for i in Mapping.item_root:
            order_info[i] = order[i]
        direction = 1 if order_info['side'] == 'BUY' else -1
        indicator = order['indicators'][0]

        order_info['direction'] = direction * -1 if order_info['tradeType'] in ['Market', 'Stop'] else direction

        if order['status'] == 'WAITING':
            if (order['indicatorType'] == 'OPEN') and (order_info['tradeType'] == 'Market'):
                order_info['price'] = indicator['triggerPrice'] - self.ticksize[order_info['symbol']]['tick'] * order_info['direction']
            else:
                order_info['price'] = indicator['triggerPrice']
        elif order['status'] == 'PENDING':
            if order_info['tradeType'] == 'Market':
                return
            order_info['price'] = indicator['cancelPrice']  # PENDING
            order_info['direction'] = direction * -1
        else:
            print('Unknown status', order)
            return

        res = self.db_set_order(order_info)
        return res

    @except_console
    def set_trendline_queue(self, order_info):
        start_waiting_time = None
        if DEBUG: print(str(datetime.now()), 'SET_TRENDLINE: {}'.format(str(order_info)))
        if order_info['indicator'] == 'trendLine':
            _now = get_unixtime()
            print('SETQ_func', order_info)

            if order_info['indicatorType'] == 'TRAIL':
                action = 'CLOSE'
            else:
                action = 'TRIGGER_CANCEL' if order_info['status'] == 'PENDING' else 'OPEN'
            _key = '{}={}'.format(order_info['id'], action)

            if _now > order_info['endDate']:
                print('now:', _now, order_info['endDate'])
                self.db_post_order(order_info, 'END')
                return None

            if int(order_info['startDate']) < _now:

                _tmp_queue = {i: order_info[i] for i in Mapping.item_indicators['trendLine']}
                _tmp_queue['symbol'] = order_info['symbol']
                _tmp_queue['currentPrice'] = 0
                _tmp_queue['direction'] = order_info['direction']
                self.db_set_line_queue(_key, _tmp_queue)

            else:
                start_waiting_time = int(order_info['startDate'])

        return start_waiting_time

    @except_console
    def set_trendline(self, _id, order):
        if (order['status'] == 'PENDING') and (order['tradeType'] == 'Market'):
            return

        order_info = {}
        order_info['indicator'] = order['planType']
        order_info['symbol'] = order['symbol']
        if DEBUG: print(str(datetime.now()), 'SET_TRENDLINE: {}'.format(str(order)))

        for i in Mapping.item_root:
            order_info[i] = order[i]
        indicator = order['indicators'][0]
        order_info['direction'] = -1 if order_info['side'] == 'BUY' else 1
        for i in Mapping.item_indicators['trendLine']:
            if 'Date' in i:
                order_info[i] = change_unixtime(indicator[i])
            elif 'Price' in i:
                order_info[i] = float(indicator[i])

        print('SET_Q:', order_info)
        start_waiting_time = self.set_trendline_queue(order_info)

        self.db_set_order(order_info, start_waiting_time)
        return

    @except_console
    def set_strategy(self, _id, order):

        order_info = {i: order[i] for i in Mapping.item_root}
        order_info['direction'] = 1 if order_info['side'] == 'BUY' else -1
        order_info['indicators'] = order[Mapping.indicator_key]
        if 'candleSize' in order_info['indicators'][0]:
            # indicator order
            order_info['candleSize'] = order_info['indicators'][0]['candleSize']
        else:
            # limit order / price trigger 등록
            direction = 1 if order_info['side'] == 'BUY' else -1
            indicator = order['indicators'][0]
            if order['status'] == 'WAITING':
                order_info['direction'] = direction * -1 if order_info['tradeType'] == 'Market' else direction
                order_info['price'] = indicator['triggerPrice']
            elif order['status'] == 'PENDING':
                order_info['price'] = indicator['cancelPrice']  # PENDING
                order_info['direction'] = direction * -1
        print('set_indicator')
        res = self.db_set_order(order_info)
        return res


class TimeWatcher(BaseDb):
    """
    trendline 주문의 start, endtime 모니터링
    """
    def __init__(self):
        super().__init__()

    def run(self):
        while True:
            self.set_price()
            sleep(10)

    @except_console
    def set_price(self):
        _now = get_unixtime()
        end_res, end_ids = self.db_end_time_check(_now)
        start_res, start_ids = self.db_start_time_check(_now)
        if end_res:
            print(str(datetime.now()), 'END_TIME:', end_ids)
        if start_res:
            print(_now, 'START_TIME:', start_ids, start_res)


class PriceWatcher(BaseDb):
    """
    price_queue를 통해 들어오는 price를 기준으로 주문 check
    """
    def __init__(self):
        super().__init__()
        self.before_price = {}
        self.price_q = None
        self.cnt = 0

    def run(self, price_q):
        self.price_q = price_q
        while True:
            self.scan_order()

    @except_console
    def scan_order(self):
        _row = self.price_q.get()
        current_price = json.loads(_row)
        start = datetime.now()
        _symbol = str(current_price['symbol'])
        current_price['price'] = float(current_price['price'])

        if DEBUG and (_symbol == 'BTC-USDT'):
            self.cnt += 1
            if self.cnt > 100:
                print(_symbol, current_price['price'])
                self.cnt = 0
        if _symbol not in self.before_price:
            self.before_price[_symbol] = current_price['price']
        else:
            if self.before_price[_symbol] > current_price['price']:
                res = self.db_scan_decrease(current_price['symbol'], current_price['price'])
                self.before_price[_symbol] = current_price['price']
            elif self.before_price[_symbol] <= current_price['price']:
                res = self.db_scan_increase(current_price['symbol'], current_price['price'])
                self.before_price[_symbol] = current_price['price']


class LineWatcher(BaseDb, MessageHandle):
    def __init__(self, ):
        super().__init__()
        self.orders_dict = None
        self.ticksize = self.get_ticksize()
        self.set_score_list = None

    def run(self):
        while True:
            self.set_score_list = {i: defaultdict(dict) for i in self.ticksize}
            self.price_update()
            sleep(10)

    @except_console
    def price_update(self):
        _now = get_unixtime()
        for _id, order in self.db_get_trendline_queue().items():
            order = json.loads(order)
            calc_price = trendline_trigger_calc(_now, **order)
            current_price = round_coin(calc_price, self.ticksize[order['symbol']]['tick'],
                                       self.ticksize[order['symbol']]['length']) + order['direction'] * self.ticksize[order['symbol']]['tick']
            if order['currentPrice'] != current_price:

                self.set_score_list[order['symbol']][order['direction']][_id] = current_price
        for symbol, rows in self.set_score_list.items():
            for direction, row in rows.items():
                self.db_change_score(symbol, direction, row)


if __name__ == "__main__":
    logger = logging.getLogger('WatchDog')
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    while True:
        o = WatchDog()
        o.run()
        sleep(3)
        print('Watchdog restart')

