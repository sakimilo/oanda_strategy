import pandas as pd
import numpy as np
import json
import oandapy as opy
import matplotlib.pyplot as plt
import seaborn as sns
from lib import MomentumTrader

config  = json.load(open('./config/oanda_config.json'))
oanda   = opy.API(environment="practice", access_token=config['oanda_login']['access_token'])

if __name__ == '__main__':

    print('fetching historical data')
    response = oanda.get_prices(instruments="EUR_USD")
    data     = oanda.get_history(instrument='EUR_USD',
                                 start='2018-04-01',
                                 end='2018-04-03',
                                 granularity='M1')
    df       = pd.DataFrame(data['candles']).set_index('time')
    df.index = pd.DatetimeIndex(df.index)

    print('calculate returns')
    df['returns'] = np.log(df['closeAsk'] / df['closeAsk'].shift(1)) 
    cols          = []

    print('simulate momentum')
    for momentum in [15, 30, 60, 120]:
        col     = 'position_%s' % momentum
        df[col] = np.sign(df['returns'].rolling(momentum).mean())
        cols.append(col)

    strats = ['returns']

    print('simulate strategy')
    for col in cols:
        strat     = 'strategy_%s' % col.split('_')[1]
        df[strat] = df[col].shift(1) * df['returns']
        strats.append(strat)

    print('plot returns')
    sns.set()
    df[strats].dropna().cumsum().apply(np.exp).plot()
    plt.savefig('./results/strategy.png', dpi=300)

    print('fetching realtime data')
    stream_ = opy.Streamer(environment='practice', access_token=config['oanda_login']['access_token'])
    # params  = {'accountId': config['oanda_login']['account_id'], 'instruments': 'AUD_CAD,AUD_CHF'}
    # stream_.run(endpoint='v1/prices', params=params)
    catch = stream_.rates(account_id=config['oanda_login']['account_id'], instruments="EUR_USD")


    if False:

        mt = MomentumTrader(momentum=12, environment='practice',
                            access_token=config['oanda_login']['access_token'])
        catch = mt.rates(account_id=config['oanda_login']['account_id'], instruments='DE30_EUR', ignore_heartbeat=True)

        request_args           = {}
        request_args['params'] = {'instruments': 'AUD_CAD,AUD_CHF'}

        import requests
        client = requests.Session()
        client.headers['Authorization'] = 'Bearer ' + config['oanda_login']['access_token']

        response = client.get('https://api-fxpractice.oanda.com/v1/prices', **request_args)
        content  = response.content.decode('utf-8')
        content  = json.loads(content)
        print(content)
