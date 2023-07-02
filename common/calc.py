import time
from datetime import datetime


def get_unixtime():
    """
    현재 시간의 unixtime 반환
    :return: 1595203200000
    """
    return int(time.mktime(datetime.now().timetuple())) * 1000


def change_unixtime(date):
    """
    date string을 unixtime으로 반환
    :param date: "2020-07-06T11:23:49.000Z"
    :return: 1594034629000
    """
    return int(time.mktime(datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%fZ').timetuple())) * 1000


def trendline_trigger_calc(current_time, tradingStartPrice=0, tradingEndPrice=0, startDate=0, endDate=0, symbol=None, currentPrice=None, direction=None):
    """
    trendline 현재가격 계산
    :param current_time: unixtime
    :param tradingStartPrice: price
    :param tradingEndPrice: price
    :param startDate: unixtime
    :param endDate: unixtime
    :param symbol: None
    :param currentPrice: None
    :param direction: None
    :return: price
    """
    return tradingStartPrice + (current_time - startDate) / (endDate - startDate) * (tradingEndPrice - tradingStartPrice)


def round_coin(price, size, length):
    """
    입력값을 tick size 단위로 변환
    :param price: 9876.342
    :param size: 0.05
    :param length: 2 # 소숫점 자릿수
    :return: 9876.35
    """
    return round(round(float(price) / float(size)) * float(size), length)

