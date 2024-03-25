import pandas as pd
import matplotlib.pyplot as plot

# dfi = pd.read_feather('old_ridership_in.ftr')
# dfiw = dfi.resample('W', on='dt').sum()
# plot.plot(dfiw.loc['2020-01-01':])
# plot.show()
# plot.clf()


def chart(freq='W', since='2020-01-01'):
    fig, axs = plot.subplots(2, sharex=True, sharey=True)
    fig.suptitle('NYC MTA Weekly Ridership by Station Complex')
    axs[0].plot(pd.read_feather('ridership_in.ftr').resample(freq, on='dtidx').sum().loc[since:])
    axs[0].set_ylabel('Entrances')
    axs[1].plot(pd.read_feather('ridership_out.ftr').resample(freq, on='dtidx').sum().loc[since:])
    axs[1].set_ylabel('Exits')
    plot.plot()
    plot.show()
    plot.clf()

chart(freq='W', since='2019-08-04')
