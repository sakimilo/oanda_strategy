import pandas as pd
import numpy as np
import json
from oandapyV20 import API
import oandapyV20.endpoints.forexlabs as labs
import oandapyV20.endpoints.accounts as accounts
from oandapyV20.endpoints.pricing import PricingStream
from datetime import datetime
from dateutil import tz
import time

def get_instruments(client, accountID):

    r           = accounts.AccountInstruments(accountID=accountID)
    rv          = client.request(r)
    data        = pd.DataFrame(rv['instruments'])

    return data

def get_signals(client):

    print('getting signals')
    params          = {}
    r               = labs.Autochartist(params=params)
    client.request(r)
    rv              = r.response
    signals         = pd.DataFrame(rv['signals'])

    metakeys        = signals.loc[0, 'meta'].keys()
    datakeys        = signals.loc[0, 'data'].keys()
    for m in metakeys:
        signals[m]  = signals.apply(lambda x: x['meta'][m], axis=1)
    for d in datakeys:
        signals[d]  = signals.apply(lambda x: x['data'][d], axis=1)
    signals         = signals[[c for c in signals.columns if c not in ['meta', 'data']]]
    
    scorekeys       = signals.loc[0, 'scores'].keys()
    for s in scorekeys:
        signals[s]  = signals.apply(lambda x: x['scores'][s], axis=1)
    signals         = signals[[c for c in signals.columns if c not in ['scores']]]

    historykeys     = signals.loc[0, 'historicalstats'].keys()
    predictionkeys  = signals.loc[0, 'prediction'].keys()
    for h in historykeys:
        signals[h]  = signals.apply(lambda x: x['historicalstats'][h], axis=1)
    for p in predictionkeys:
        signals[p]  = signals.apply(lambda x: x['prediction'][p], axis=1)
    signals         = signals[[c for c in signals.columns if c not in ['historicalstats', 'prediction']]]

    for c in ['pattern', 'symbol', 'hourofday']:
        signals[c + '_percent'] = signals.apply(lambda x: x[c]['percent'], axis=1)
    signals         = signals[[c for c in signals.columns if c not in ['pattern', 'symbol', 'hourofday']]]

    pointskey       = signals.loc[0, 'points'].keys()
    for p in pointskey:
        signals[p]  = signals.apply(lambda x: x['points'][p], axis=1)
    signals         = signals[[c for c in signals.columns if c not in ['points']]]

    for c in ['support', 'resistance']:
        signals[c + '_y0'] = signals.apply(lambda x: x[c]['y0'], axis=1)
        signals[c + '_y1'] = signals.apply(lambda x: x[c]['y1'], axis=1)
        signals[c + '_x0'] = signals.apply(lambda x: x[c]['x0'], axis=1)
        signals[c + '_x1'] = signals.apply(lambda x: x[c]['x1'], axis=1)
    signals         = signals[[c for c in signals.columns if c not in ['support', 'resistance']]]

    time_cols       = ['patternendtime', 'timefrom', 'timeto', 'support_x0', 'support_x1', 
                       'resistance_x0', 'resistance_x1']
    for c in time_cols:
        signals[c]  = signals[c].apply(lambda t: convert_ToLocal(convert_UnixTime(t)))

    return signals

def get_streaming_price(client, accountID, ls_instrument):

    print('getting {} instruments'.format(len(ls_instrument)))

    params          = {"instruments": ",".join(ls_instrument)}
    r               = PricingStream(accountID=accountID, params=params)
    
    price_ls        = []
    n               = 0
    max_iter        = len(ls_instrument)

    for R in client.request(r):
        price_ls.append(R)
        n    += 1
        if n >= max_iter:
            break

    price_data         = pd.DataFrame(price_ls)

    for c in ['asks', 'bids']:
        price_data[c]  = price_data[c].apply(lambda x: x[0]['price'])

    price_data['time'] = pd.to_datetime(price_data['time'])
    price_data['time'] = price_data['time'].apply(lambda t: convert_ToLocal(t))

    instrument_to_ask  = dict(zip(price_data['instrument'], price_data['asks']))
    instrument_to_bid  = dict(zip(price_data['instrument'], price_data['bids']))

    return instrument_to_ask, instrument_to_bid

def convert_UnixTime(t):

    utc     = datetime.utcfromtimestamp(t)
    return utc

def convert_ToLocal(datetime_utc):

    from_zone       = tz.gettz('UTC')
    to_zone         = tz.gettz('Singapore')
    datetime_utc    = datetime_utc.replace(tzinfo=from_zone)
    local           = datetime_utc.astimezone(to_zone)
    return local

def predict_price(row):

    if row['direction'] == 1:
        return row['pricelow']
    
    elif row['direction'] == -1:
        return row['pricehigh']

def make_datatype(df, c_int=True, c_object=True, c_float=True, c_time=True):

    int_cols            = ['id', 'interval', 'direction', 'length', 'completed', 'clarity', 'initialtrend', 
                           'breakout', 'quality', 'uniformity']
    object_cols         = ['instrument', 'type', 'trendtype']
    float_cols          = ['probability', 'pricelow', 'pricehigh', 
                           'pattern_percent', 'symbol_percent', 'hourofday_percent', 
                           'support_y0', 'support_y1', 'resistance_y0', 'resistance_y1']
    datetime_cols       = ['patternendtime', 'timefrom', 'timeto', 'support_x0', 'support_x1', 
                           'resistance_x0', 'resistance_x1']

    if c_int:
        df[int_cols]    = df[int_cols].astype(np.int64)

    if c_object:
        df[object_cols] = df[object_cols].astype(object)

    if c_float:
        df[float_cols]  = df[float_cols].astype(np.float64).round(3)

    if c_time:
        for timecol in datetime_cols:
            df[timecol] = df[timecol].apply(lambda t: pd.to_datetime(t))
            df[timecol] = df[timecol].apply(lambda t: convert_ToLocal(t))

    return df

if __name__ == '__main__':

    config              = json.load(open('./config/oanda_config.json'))
    accountID           = config['practice_login']['account_id']
    access_token        = config['practice_login']['access_token']
    machine_utc_time    = config['machine']['utc_time']

    client              = API(access_token=access_token, environment="practice")
    count               = 0
    max_iter            = 10000

    while True:

        try:
            if count > max_iter:
                print('reached max_iter', max_iter)
                break

            count              += 1
            hist_signals        = pd.read_csv('./results/signals.csv')
            hist_signals        = hist_signals[[c for c in hist_signals.columns if c not in \
                                                                ['curr_ask', 'curr_bid', 'pred']]]
            hist_signals        = make_datatype(hist_signals)

            signals             = get_signals(client)
            signals             = make_datatype(signals, c_time=False)
            ls_instrument       = signals['instrument'].as_matrix()
            signals             = pd.concat([hist_signals, signals])
            signals             = signals.drop_duplicates()

            stream_output       = get_streaming_price(client, accountID, ls_instrument)
            instrument_to_ask   = stream_output[0]
            instrument_to_bid   = stream_output[1]

            signals['curr_ask'] = signals['instrument'].map(instrument_to_ask)
            signals['curr_bid'] = signals['instrument'].map(instrument_to_bid)
            signals['pred']     = signals.apply(lambda x: predict_price(x), axis=1)

            signals.to_csv('./results/signals.csv', index=False)
            print('{}) finished fetching signals, shape: {}'.format(count, signals.shape))

            time.sleep(60 * 5)

        except Exception as e:

            if machine_utc_time:
                current_time    = convert_ToLocal(datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
            else:
                current_time    = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            with open("./results/errors.log", "a") as LOG:
                LOG.write("{}, V20Error: {}\n".format(current_time, e))

    print('done')
    
