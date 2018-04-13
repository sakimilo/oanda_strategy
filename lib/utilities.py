import json
from oandapyV20 import API
from oandapyV20.exceptions import V20Error
import oandapyV20.endpoints.instruments as instruments
from oandapyV20.definitions.instruments import CandlestickGranularity
import pandas as pd
import numpy as np
from datetime import datetime
from dateutil import tz

config     = json.load(open('./config/oanda_config.json'))

class PriceHist(object):

    def __init__(self, instruments_i):

        self.config         = config
        self.accountID      = self.config['practice_login']['account_id']
        self.access_token   = self.config['practice_login']['access_token']
        self.params         = self.config['candlestick_params']
        self.api            = API(access_token=self.access_token, environment="practice")
        self.instrument     = instruments_i

        if self.params['count']: del self.params['count']

    def convert_ToLocal(self, datetime_utc):

        from_zone           = tz.gettz('UTC')
        to_zone             = tz.gettz('Singapore')
        datetime_utc        = datetime_utc.replace(tzinfo=from_zone)
        local               = datetime_utc.astimezone(to_zone)
        
        return local

    def make_datatype(self, df, c_int=True, c_object=True, c_float=True, c_time=True):

        int_cols            = ['volume']
        object_cols         = []
        float_cols          = ['open', 'high', 'low', 'close']
        datetime_cols       = ['time']

        if c_int:
            df[int_cols]    = df[int_cols].astype(np.int64)

        if c_object:
            df[object_cols] = df[object_cols].astype(object)

        if c_float:
            df[float_cols]  = df[float_cols].astype(np.float64).round(3)

        if c_time:
            for timecol in datetime_cols:
                df[timecol] = df[timecol].apply(lambda t: pd.to_datetime(t))
                df[timecol] = df[timecol].apply(lambda t: self.convert_ToLocal(t))

        return df

    def get_highlow(self):

        r                   = instruments.InstrumentsCandles(instrument=self.instrument, params=self.params)
        rv                  = self.api.request(r)

        data                = pd.DataFrame(rv['candles'])
        data['open']        = data.apply(lambda x: x['mid']['o'], axis=1)
        data['high']        = data.apply(lambda x: x['mid']['h'], axis=1)
        data['low']         = data.apply(lambda x: x['mid']['l'], axis=1)
        data['close']       = data.apply(lambda x: x['mid']['c'], axis=1)
        data                = data[['time', 'volume', 'open', 'high', 'low', 'close']]
        data                = self.make_datatype(data)

        return data['high'].max(), data['low'].min()



