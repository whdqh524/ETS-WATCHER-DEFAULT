
class Mapping:

    item_root = ['id', 'active', 'status', 'side', 'tradeType', 'indicatorType', 'symbol', 'planType']
    indicator_key = 'indicators'
    item_indicators = {
        'default': ['triggerPrice', 'cancelPrice', 'symbol'],
        'trendLine': ['startDate', 'endDate', 'tradingStartPrice', 'tradingEndPrice']
    }

    date_ckeck = ['trendLine']
    active_code = [1, 4, 5]


class IndicatorMap:
    indicator_map = {
        'macd': {'config': ['name', 'fastMaPeriod', 'slowMaPeriod', 'signalPeriod'], 'value': []},
        'ema_cross': {'config': ['name', 'shortPeriod', 'longPeriod'], 'value': []},
        'stoch': {'config': ['name', 'kPeriod', 'smoothK', 'dPeriod'], 'value': ['overValue']},
        'stoch_rsi': {'config': ['name', 'rsiPeriod', 'kPeriod', 'smoothK', 'dPeriod'], 'value': ['overValue']},
        'rsi': {'config': ['name', 'period'], 'value': ['overValue']},
        'mfi': {'config': ['name', 'period'], 'value': ['overValue']},
        'bollinger_band': {'config': ['name', 'period', 'deviations'], 'value': ['band']},
        'vma': {'config': ['name', 'period'], 'value': ['rate']},
        'obv_cross': {'config': ['name', 'shortPeriod', 'longPeriod'], 'value': []},
        'supertrend': {'config': ['name', 'period', 'multiplier'], 'value': []},
        'tii': {'config': ['name', 'period', 'signalPeriod'], 'value': ['overValue']},
        'vma_cross': {'config': ['name', 'shortPeriod', 'longPeriod'], 'value': []},
        'atr_trailing_stop': {'config': ['name', 'period', 'multiplier', 'highlow'], 'value': []}
    }

