import pandas as pd

i = pd.read_feather('ridership_data/ridership_in.ftr').resample('W', on='dtidx').sum().reset_index()
i = i.set_index(pd.MultiIndex.from_tuples(i['dtidx'].apply(lambda x: x.isocalendar()[0:2])))

o = pd.read_feather('ridership_out.ftr').set_index('dtidx').resample('W', on='dtidx').sum()
o = o.set_index(o['dtidx'].apply(lambda x: x.isocalendar()[0:2]))

print(i)
print(o)
