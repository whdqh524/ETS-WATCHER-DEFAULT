import ujson as json
import redis
from config.settings import Redis, Watcher
from config.trading_map import Mapping, IndicatorMap
from common.msg import MessageHandle
from common.decorator import except_console, except_pass


class RedisClient(object):
    def __init__(self):
        if Redis.REDIS_PASSWORD:
            self.pool = redis.ConnectionPool(host=Redis.REDIS_SERVER, db=Redis.REDIS_DB, port=Redis.REDIS_PORT,
                                             password=Redis.REDIS_PASSWORD, decode_responses=True)
        else:
            self.pool = redis.ConnectionPool(host=Redis.REDIS_SERVER, db=Redis.REDIS_DB, port=Redis.REDIS_PORT,
                                             decode_responses=True)

    def conn(self):
        return redis.StrictRedis(connection_pool=self.pool)


class BaseDb(MessageHandle, Mapping):
    def __init__(self):
        MessageHandle.__init__(self)
        Mapping.__init__(self)
        self.client = RedisClient().conn()
        self.scan_direction = {'buy': '+', 'sell': '-'}
        self.logger = None

    @except_console
    def db_get_price(self):
        """
        socket 에서 push해준 가격을 가져옴
        :return: '{"symbol": "XBT-USD", "price": 9845.5}'
        """
        price = self.client.blpop(Watcher.PRICE_QUEUE)
        return price[1]

    @except_console
    def db_get_order(self):
        """
        new order id를 가져와 order 정보를 반환
        :return: id, dict(order_detail)
        """
        _id = self.client.blpop(Watcher.NEW_ORDER)[1]
        # res = self.db_zrem(Watcher.TRANSACTION, _id)
        order_detail = self.client.hget(Watcher.ORDER_DETAIL, _id)
        print(Watcher.ORDER_DETAIL, _id, '\n', order_detail)
        return _id, json.loads(order_detail)

    @except_console
    def db_get_order_info(self, _id):
        return json.loads(self.client.hget(Watcher.ORDER_DETAIL, _id))

    @except_console
    def db_get_trendline_queue(self):
        res = self.client.hgetall(Watcher.TRENDLINE_QUEUE)
        if not res:
            res = {}
        return res

    @except_console
    def db_order_list_getall(self):
        order_list = self.client.hgetall(Watcher.MTS_ORDER_LIST)
        return order_list

    @except_console
    def db_set_order_info(self, _order_info):
        res = self.client.hset(Watcher.MTS_ORDER_LIST, _order_info['id'], json.dumps(_order_info))
        return res

    @except_console
    def db_set_line_queue(self, _key, data):
        res = self.client.hset(Watcher.TRENDLINE_QUEUE, _key, json.dumps(data))
        return res

    @except_console
    def db_rm_line_queue(self, _key):
        res = self.db_hdel(Watcher.TRENDLINE_QUEUE, _key)
        return res

    @except_console
    def db_set_order(self, order_info, start_waiting_time=None):
        if order_info['planType'] != 'strategy':
            if order_info['indicatorType'] == 'OPEN':
                if order_info['status'] == 'PENDING':
                    action = 'TRIGGER_CANCEL'
                elif order_info['status'] == 'WAITING':
                    action = 'OPEN'
            else:
                if order_info['status'] == 'PENDING':
                    action = 'TRIGGER_CANCEL'
                elif order_info['status'] == 'WAITING':
                    action = 'CLOSE'
                else:
                    print('invailid status:', order_info['status'])
                    return
            _key = '{}={}'.format(order_info['id'], action)

            p = self.client.pipeline(transaction=True)
            if order_info['planType'] in Mapping.date_ckeck and action == 'OPEN':
                # start, end time 설정
                if start_waiting_time:
                    p.zadd(Watcher.START_TIME_MON, {order_info['id']: start_waiting_time})
                p.zadd(Watcher.END_TIME_MON, {order_info['id']: order_info['endDate']})

            if 'price' in order_info:
                # price triger 등록
                p.zadd(Watcher.WATCHER_LIST.format(order_info['symbol'], order_info['direction']),
                       {_key: float(order_info['price'])})
            p.execute()

        else:
            # strategy 주문 저장
            if 'candleSize' in order_info:
                # indicator 주문
                _symbol_kline = '{}_{}'.format(order_info['symbol'], order_info['candleSize'])
                indicator_dic = {'indicators': {}}
                for indicator in order_info['indicators']:
                    _items_key = IndicatorMap.indicator_map[indicator['name']]['config'][1:]
                    _value_key = IndicatorMap.indicator_map[indicator['name']]['value']
                    indicator_key = '|'.join([indicator[IndicatorMap.indicator_map[indicator['name']]['config'][0]]] + [str(float(indicator[i])) for i in _items_key])
                    values = [float(indicator[i]) for i in _value_key]
                    indicator_dic['indicators'][indicator_key] = values
                    self.set_indicator(_symbol_kline, order_info['id'], indicator_key)

                indicator_dic['direction'] = order_info['direction']
                indicator_dic['last_side'] = None
                indicator_dic['indicatorType'] = order_info['indicatorType']
                self.set_indicator_order(_symbol_kline, order_info['id'], indicator_dic)
            else:
                # limit 주문
                if order_info['indicatorType'] == 'TAKE':
                    if order_info['status'] == 'PENDING':
                        action = 'TRIGGER_CANCEL'
                    elif order_info['status'] == 'WAITING':
                        action = 'CLOSE'
                    else:
                        print('invailid status:', order_info['status'])
                        return
                    _key = '{}={}'.format(order_info['id'], action)
                    if 'price' in order_info:
                        self.client.zadd(Watcher.WATCHER_LIST.format(order_info['symbol'], order_info['direction']),
                                         {_key: float(order_info['price'])})
                elif order_info['indicatorType'] == 'LOSS':
                    if order_info['status'] != 'WAITING':
                        print('invailid status:', order_info['status'])
                        return
                    action = 'CLOSE'
                    _key = '{}={}'.format(order_info['id'], action)
                    if 'price' in order_info:
                        self.client.zadd(Watcher.WATCHER_LIST.format(order_info['symbol'], order_info['direction']),
                                         {_key: float(order_info['price'])})

        res = self.db_set_order_info(order_info)
        return res

    @except_console
    def set_indicator(self, _symbol_kline, ord_id, indicator, remove=False):
        """
        인티케이터 추가/삭제
        :param _symbol_kline:  'ETH-USD_15'
        :param ord_id: '2as12-3asdfasdf-3-asdf-asdfaf'
        :param indicator: 'macd|10|20'
        :param remove: True / False
        :return: 1/0 의미없음
        """
        res = None
        if not remove:
            # indicator 목록제거
            data = self.client.hget(Watcher.INDICATOR_LIST.format(_symbol_kline), indicator)
            if data:
                indicator_set = eval(data) # string 을 set type으로 변환
                indicator_set.add(ord_id)
            else:
                indicator_set = {ord_id}
            res = self.client.hset(Watcher.INDICATOR_LIST.format(_symbol_kline), indicator, str(indicator_set))
        else:
            # indicator 추가
            data = self.client.hget(Watcher.INDICATOR_LIST.format(_symbol_kline), indicator)
            if data:
                indicator_set = eval(data) # string 을 set type으로 변환
                indicator_set.remove(ord_id)
                if indicator_set:
                    res = self.client.hset(Watcher.INDICATOR_LIST.format(_symbol_kline), indicator, str(indicator_set))
                else:
                    res = self.db_hdel(Watcher.INDICATOR_LIST.format(_symbol_kline), indicator)
        return res

    @except_console
    def set_indicator_order(self, _symbol_kline, _id, indicator_dic=None, remove=False):
        """
        주문 추가/삭제
        :param _symbol_kline:  'ETH-USD_15'
        :param ord_id: '2as12-3asdfasdf-3-asdf-asdfaf'
        :param indicator_dic: 'macd|10|20'
        :param remove: True / False
        :return: 1/0 의미없음
        """
        res = {}
        if not remove:
            # 주문 삭제
            res['set_{}'.format(_id)] = self.client.hset(Watcher.MTS3_ORDER_LIST.format(_symbol_kline), _id, json.dumps(indicator_dic))
        else:
            # 주문 추가
            indicator_list = self.client.hget(Watcher.MTS3_ORDER_LIST.format(_symbol_kline), _id)
            if indicator_list:
                indicator_list = json.loads(indicator_list)
                for i in indicator_list['indicators']:
                    res = self.set_indicator(_symbol_kline, _id, i, remove=True)
        return res

    @except_pass
    def db_zrem(self, _key, _id):
        res = self.client.zrem(_key, _id)

    @except_pass
    def db_hdel(self, _key, _id):
        self.client.hdel(_key, _id)

    @except_console
    def db_post_order(self, order_info, action):
        """
        주문처리
        :param order_info: dict(주문 정보)
        :param action: 'OPEN', 'TAKE', 'CLOSE'
        :return:
        """
        _key = '{}={}'.format(order_info['id'], action)

        print('db_post_order:', _key)
        self.rm_order(order_info['id'], order_info['symbol'])

        p = self.client.pipeline(transaction=True)

        if action == 'END':
            p.rpush(Watcher.POST_ORDER, '{}={}'.format(order_info['id'], action))
        else:
            p.rpush(Watcher.POST_ORDER, '{}={}'.format(order_info['id'], action))

        print('POST_RES:','{}={}'.format(order_info['id'], action))
        res = p.execute()

        return res

    @except_console
    def rm_order(self, _id, symbol, indicators=False):
        # trendline 인경우 삭제
        for _key in self.client.hscan_iter(Watcher.TRENDLINE_QUEUE, match= _id + '*'):
            self.db_hdel(Watcher.TRENDLINE_QUEUE, _key[0])

        # buy, sell triger 삭제
        buy_list = self.client.zscan(Watcher.WATCHER_LIST.format(symbol, 1), 0, '*{}*'.format(_id), 100000000)
        sell_list = self.client.zscan(Watcher.WATCHER_LIST.format(symbol, -1), 0, '*{}*'.format(_id), 100000000)
        for i in buy_list[1]:
            self.db_zrem(Watcher.WATCHER_LIST.format(symbol, 1), i[0])
        for i in sell_list[1]:
            self.db_zrem(Watcher.WATCHER_LIST.format(symbol, -1), i[0])

        # 주문정보 삭제
        self.db_hdel(Watcher.MTS_ORDER_LIST, _id)
        if indicators and 'candleSize' in indicators[0]:
            _symbol_kline = '{}_{}'.format(symbol, indicators[0]['candleSize'])
            self.db_hdel(Watcher.MTS3_ORDER_LIST.format(_symbol_kline), _id)

        self.db_hdel(Watcher.MTS_ORDER_DETAIL, _id)

        #trendline인 경우 start, end time 삭제
        self.db_hdel(Watcher.START_TIME_MON, _id)
        self.db_hdel(Watcher.END_TIME_MON, _id)

    @except_console
    def db_change_score(self, symbol, direction, items_dict):
        _key = Watcher.WATCHER_LIST.format(symbol, direction)
        p = self.client.pipeline(transaction=True)
        for i in items_dict:
            p.zadd(_key, {i: items_dict[i]}, ch=True)
        res = p.execute()
        return res

    @except_console
    def db_start_time_check(self, _now):
        res = []
        target = self.client.zrangebyscore(Watcher.START_TIME_MON, '-inf', _now, withscores=False)
        if target:
            order_list = self.client.hmget(Watcher.MTS_ORDER_LIST, *target)
            if order_list and order_list[0] != None:
                for order in order_list:
                    order = json.loads(order)
                    res.append(self.db_set_order(order, start_waiting_time=None))
        return res, target

    @except_console
    def db_end_time_check(self, _now):
        res = []
        target = self.client.zrangebyscore(Watcher.END_TIME_MON, '-inf', _now, withscores=False)
        if target:
            order_list = self.client.hmget(Watcher.MTS_ORDER_LIST, *target)
            if order_list[0]:
                try:
                    for order in order_list:
                        order = json.loads(order)
                        res.append(self.db_post_order(order, 'END'))
                        print('order:', order)
                except Exception as e:
                    print('db_end_time_check for order_list error:', e)

        return res, target

    @except_console
    def db_scan_decrease(self, symbol=None, price=None):
        """
        하락 매수 triger 목록 scan
        :param symbol:
        :param price:
        :return:
        """
        res = []
        _key = Watcher.WATCHER_LIST.format(symbol, '1')
        target = self.client.zrangebyscore(_key, price, '+inf', withscores=True)
        if target:
            for _key, score in target:
                _id, action = _key.split('=')
                order = self.client.hget(Watcher.MTS_ORDER_LIST, _id)
                if order:
                    self.db_rm_line_queue(_key)
                    res.append(self.db_post_order(json.loads(order), action))
                else:
                    print('db_scan_buy cannot findid', Watcher.MTS_ORDER_LIST, _key, _id)
                    # self.send_slack('PriceWatcher scan_buy', 'can not find id', _key)
            return res

    @except_console
    def db_scan_increase(self, symbol=None, price=None):
        """
        상승 매도 triger 목록 scan
        :param symbol:
        :param price:
        :return:
        """
        res = []
        _key = Watcher.WATCHER_LIST.format(symbol, '-1')
        target = self.client.zrangebyscore(_key, '-inf', price, withscores=True)
        if target:
            for _key, score in target:
                _id, action = _key.split('=')
                order = self.client.hget(Watcher.MTS_ORDER_LIST, _id)
                if order:
                    res.append(self.db_post_order(json.loads(order), action))
                else:
                    print('db_scan_buy cannot findid', Watcher.MTS_ORDER_LIST, _key, _id)
                    # self.send_slack('PriceWatcher scan_buy', 'can not find id', _key)
            return res


    @except_console
    def get_ticksize(self):
        ticksize = {}
        keys = self.client.smembers(Watcher.TICK_SIZE_KEYS)
        for key in keys:
            info = key.split(':')
            symbol = info[0]
            tmp = self.client.hget(Watcher.MARKET_DATA_KEY, symbol)
            marketData = json.loads(tmp)
            length = 1
            ticksize[symbol] = {'tick': float(marketData['tickSize']), 'length': length}
        return ticksize


if __name__ == "__main__":
    o = BaseDb()
    # o.rm_indicator('BTC-USDT', 'user09:5e1d419db3f4c12aa0b3d716')

