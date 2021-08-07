import pandas as pd
import matplotlib.pyplot as plot
import numpy as np

diffs = pd.read_feather('diffs.ftr')
df = diffs.sample(10000)

cols = [('i', 1, 'entrances'), ('o', 2, 'exits')]

x = 70000

for e, c, n in cols:
    
    # Frequency of diff_ph cumulative z scores
    hist, bins, _ = plot.hist(diffs[[f'cum{e}z']], bins=int(np.ceil(diffs[f'cum{e}z'].max())))
    plot.title(f'Freq of {e}diff_ph cumulative Z scores')
    plot.yscale('log')
    plot.xlabel(f'z score value')
    plot.ylabel(f'freq of {e}diff_ph cumulative z score')
    plot.savefig(f'{e}_freq.png')
    plot.clf()
    
    # Logbinned frequency of diff_ph cumulative z scores
    logbins = np.logspace(np.log10(bins[0]),np.log10(bins[-1]),len(bins))
    plot.hist(diffs[[f'cum{e}z']], bins=logbins)
    plot.title(f'Freq of {e}diff_ph cumulative Z scores, logbinned')
    plot.xscale('log')
    plot.yscale('log')
    plot.xlabel(f'z score value')
    plot.ylabel(f'freq of {e}diff_ph cumulative z score')
    plot.savefig(f'{e}_logbinned_freq.png')
    plot.clf()
    
    # Frequency of diff_ph noncumulative z scores
    plot.hist(diffs[[f'{e}z']], bins=int(np.ceil(diffs[f'{e}z'].max())))
    plot.title(f'Freq of {e}diff_ph noncumulative Z scores')
    plot.yscale('log')
    plot.xlabel(f'z score value')
    plot.ylabel(f'freq of {e}diff_ph noncumulative z score')
    plot.savefig(f'{e}_noncumulative_freq.png')
    plot.clf()
    
    df = diffs.sample(x).combine_first(diffs.nlargest(n=x, columns=f'cum{e}z', keep='all')).combine_first(diffs.nlargest(n=x, columns=f'{e}z', keep='all')).combine_first(diffs.nlargest(n=x, columns=f'{e}diff_ph', keep='all'))
    
    # Cumulative z scores vs values
    plot.scatter(df[f'cum{e}z'], df[f'{e}diff_ph'])
    plot.title(f'Largest {e}diff_ph and cumulative Z score values')
    plot.yscale('log')
    plot.xlabel(f'cumulative z score')
    plot.ylabel(f'{e}diff_ph')
    plot.savefig(f'{e}_diff_v_cumulative_z.png')
    plot.clf()
    
    # Noncumulative z scores vs values
    plot.scatter(df[f'{e}z'], df[f'{e}diff_ph'])
    plot.title(f'Largest {e}diff_ph and noncumulative Z score values')
    plot.yscale('log')
    plot.xlabel(f'noncumulative z score')
    plot.ylabel(f'{e}diff_ph')
    plot.savefig(f'{e}_diff_v_noncumulative_z.png')
    plot.clf()
    
    # Cumulative z scores vs noncumulative z scores
    plot.scatter(df[f'cum{e}z'], df[f'{e}z'])
    plot.title(f'{x} largest {e}diff_ph Z scores with random sample')
    plot.xlabel(f'cumulative z score value')
    plot.ylabel(f'noncumulative z score value')
    plot.savefig(f'{e}_largest_z_scores.png')
    plot.clf()
    
    # Logbinned cumulative z scores vs noncumulative z scores
    plot.scatter(df[f'cum{e}z'], df[f'{e}z'])
    plot.title(f'{x} largest {e}diff_ph Z scores with random sample')
    plot.xscale('log')
    plot.yscale('log')
    plot.xlabel(f'cumulative z score value')
    plot.ylabel(f'noncumulative z score value')
    plot.savefig(f'{e}_logbinned_largest_z_scores.png')
    plot.clf()

print('Plotting done.')