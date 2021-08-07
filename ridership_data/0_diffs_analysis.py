import pandas as pd
import math

subset=['cumiz', 'cumoz']

diffs = pd.read_feather('diffs.ftr')
diffs = diffs.dropna(subset=subset)

pts = 3 # decimal points of smallest bins

for t in subset:
    order = math.floor(math.log(diffs[t].max(), 10))
    bins = [0]
    _ = [([bins.append(math.pow(10,i-pts) * x) for x in list(range(1, 10))]) for i in range(order+1+pts)]
    bins.append(math.pow(10, order+1))
    cut = pd.cut(diffs[t], bins=bins, labels=[f'{l:.{pts}f}' for l in bins[1:]])
    cut = cut.rename(f'{t}_bin')
    cut.value_counts(sort=False, ascending=True).to_csv(f'{t}_bin_count.csv')
