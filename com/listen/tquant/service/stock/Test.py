# coding: utf-8

import tquant as tt
import datetime
import numpy
from com.listen.tquant.utils.Utils import Utils


security_code = '002460'

day_kline = tt.get_stock_bar(security_code, 1)
print(day_kline)
indexes_values = day_kline.index.values
the_date = None
high_max = None
low_min = None
open_first = None
close_last = None
total_amount = 0
total_vol = 0
idx_datetime = None
date0930 = datetime.datetime.now()
date0930 = date0930.replace(year=2017, month=4, day=7, hour=9, minute=30, second=00, microsecond=0)
print('date0930', date0930)
for idx in indexes_values:
    idx_datetime = datetime.datetime.utcfromtimestamp(idx.astype('O')/1e9)
    if idx_datetime >= date0930:
        amount = day_kline.at[idx, 'amount']
        if isinstance(amount, numpy.ndarray) and amount.size > 1:
            amount = amount.tolist()[0]
        amount = Utils.base_round(amount, 2)
        total_amount += amount

        vol = day_kline.at[idx, 'vol']
        if isinstance(vol, numpy.ndarray) and vol.size > 1:
            vol = vol.tolist()[0]
        vol = Utils.base_round(vol, 2)
        total_vol += Utils.base_round(vol, 2)

        high = day_kline.at[idx, 'high']
        if isinstance(high, numpy.ndarray) and high.size > 1:
            high = high.tolist()[0]
        high = Utils.base_round(high, 2)
        if high_max is None:
            high_max = high
        elif high > high_max:
            high_max = high

        low = day_kline.at[idx, 'low']
        if isinstance(low, numpy.ndarray) and low.size > 1:
            low = low.tolist()[0]
        low = Utils.base_round(low, 2)
        if low_min is None:
            low_min = low
        elif low < low_min:
            low_min = low
        print('idx_datetime', idx_datetime, 'high_max', high_max, 'high', high,
              'low_min', low_min, 'low', low)

        open = day_kline.at[idx, 'open']
        if isinstance(open, numpy.ndarray) and open.size > 1:
            open = open.tolist()[0]
        open = Utils.base_round(open, 2)
        if open_first is None:
            open_first = open

        close = day_kline.at[idx, 'close']
        if isinstance(close, numpy.ndarray) and close.size > 1:
            close = close.tolist()[0]
        close = Utils.base_round(close, 2)
        close_last = close

        the_date = (idx_datetime.strftime('%Y-%m-%d'))

if idx_datetime is not None:

    result = []
    result.append(the_date)
    result.append(total_amount * 100)
    result.append(close_last)
    result.append(high_max)
    result.append(low_min)
    result.append(open_first)
    result.append(total_vol * 100)
    print(result)


aa = tt.get_last_n_daybar(security_code, 1, 'qfq')
print('aa', type(aa))
print(aa)
