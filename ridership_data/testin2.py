from diff_processing import *

df = pd.read_feather('diffed_areas/PTH22_02-03-06.ftr')

def process_resampling(df):
    start = df.loc[(df['dt'].apply(lambda x: x.minute == 0) & df['dt'].apply(lambda x: x.second == 0)), 'dt'].min()
    minabs = df['dt'].min()
    if start is pd.NaT:
        start = minabs.floor('4H')
    while minabs < start:
        start -= pd.Timedelta('4H')
    end=df['dt'].max()
    df = df.set_index('dt')
    timeindex = pd.DataFrame(index=pd.date_range(start=start, end=end, freq='4H', name='dt'))
    df = df.combine_first(timeindex).rename_axis(index='dt')
    for e in ['i', 'o']:
        df[f'{e}diffpt'] = df[f'{e}diffpt'].fillna(method='bfill')
        df[f'{e}diff'] = df[f'{e}diff'].fillna(df[f'{e}diffpt'])
    df = df[['desc', 'idiff', 'odiff']].resample('4H', origin='start_day', offset='2H', closed='right', label='right').agg({'desc': 'first', 'idiff': 'sum', 'odiff': 'sum'})
    df.index = df.index + pd.tseries.frequencies.to_offset('-2H')
    df = df.reset_index()
    df, _ = reset_analysis_columns(df, reset_e_from_diff=True,  ignore_cols=['diffsign', 'countingup'])
    df['i'] = df['i'].astype('int')
    df['o'] = df['o'].astype('int')
    return df

process_resampling(df)