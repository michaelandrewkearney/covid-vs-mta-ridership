import pandas as pd
import matplotlib.pyplot as plt
import os
from tabulate import tabulate

ftrs = [f for f in os.listdir('diffed_areas') if f.endswith('.ftr')]
logs = {tuple(f.split('.')[0].split('_')):pd.read_feather('diffed_areas/'+f) for f in ftrs}

analysis = [(k, v.isna().any(axis=1).sum(), v['idiff'].isna().sum(), (v['idiff']!=0).sum(), v['idiff'].index.size, v['iz'].isna().sum(), v['odiff'].isna().sum(), (v['odiff']!=0).sum(), v['odiff'].index.size, v['oz'].isna().sum()) for k,v in logs.items()]

# Area/SCP : CONTROLAREA and SCP tuple the data comes from
# isNA: count of rows containing NaN
# IDna: count of NaN idiff values 
# IDnz: count of non-zero idiff values
# IDct: count of idiff values
# IZna: count of idiff z_score NaN values
# ODna: count of NaN odiff values 
# ODnz: count of non-zero odiff values
# ODct: count of odiff values
# OZna: count of odiff z_score NaN values
# IDnzPct: % of idiff values that are non-zero
# ODnzPct: % of odiff values that are non-zero
# IZnaPct: % of idiff values that are NaN
# OZnaPct: % of odiff values that are NaN

headers=['Area/SCP', 'isNA', 'IDna', 'IDnz', 'IDct', 'IZna', 'ODna', 'ODnz', 'ODct', 'OZna']

a = pd.DataFrame(analysis, columns=headers)

a['IDnzPct'] = a['IDnz']/a['IDct']
a['ODnzPct'] = a['ODnz']/a['ODct']
a['IZnaPct'] = a['IZna']/a['IDct']
a['OZnaPct'] = a['OZna']/a['ODct']

a.to_csv('diffed_area_analysis.csv', index=False)

print(tabulate(a))