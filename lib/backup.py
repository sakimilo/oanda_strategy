

class MomentumTrader(opy.Streamer):

    def __init__(self, momentum, *args, **kwargs):

        opy.Streamer.__init__(self, *args, **kwargs)
        self.ticks      = 0
        self.position   = 0
        self.df         = pd.DataFrame()
        self.momentum   = momentum
        self.units      = 100000

    def create_order(self, side, units):

        order = opy.create_order(config['oanda']['account_id'], 
            instrument='EUR_USD', units=units, side=side,
            type='market')
        print('\n', order)

    def on_success(self, data):

        self.ticks += 1
        # print(self.ticks, end=', ')
        # appends the new tick data to the DataFrame object
        self.df = self.df.append(pd.DataFrame(data['tick'],
                                 index=[data['tick']['time']]))
        # transforms the time information to a DatetimeIndex object
        self.df.index = pd.DatetimeIndex(self.df['time'])
        # resamples the data set to a new, homogeneous interval
        dfr = self.df.resample('5s').last()
        # calculates the log returns
        dfr['returns'] = np.log(dfr['ask'] / dfr['ask'].shift(1))
        # derives the positioning according to the momentum strategy
        dfr['position'] = np.sign(dfr['returns'].rolling( 
                                      self.momentum).mean())

        if dfr['position'].ix[-1] == 1:
            # go long
            if self.position == 0:
                self.create_order('buy', self.units)
            elif self.position == -1:
                self.create_order('buy', self.units * 2)
            self.position = 1

        elif dfr['position'].ix[-1] == -1:
            # go short
            if self.position == 0:
                self.create_order('sell', self.units)
            elif self.position == 1:
                self.create_order('sell', self.units * 2)
            self.position = -1

        if self.ticks == 250:
            # close out the position
            if self.position == 1:
                self.create_order('sell', self.units)
            elif self.position == -1:
                self.create_order('buy', self.units)
            self.disconnect()

