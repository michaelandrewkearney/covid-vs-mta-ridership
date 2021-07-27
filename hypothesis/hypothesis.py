import numpy as np
import pandas as pd
import sqlite3
from scipy.stats import ttest_rel

#Goals
"""
Performs a Paired 2-sample T-test on Covid Cases Percent and Percent Ridership
"""

# Function taken from Stats Assigment
def paired_ttest(values_a, values_b):
    """
    Input:
        - values_a: a 1-d array of numbers that is one set of samples
        - values_b: a 1-d array of numbers that is the other set of samples
                * Note on input: len(values_a) and len(values_b) have to be the 
                same, and samples of values_a and values_b are treated as pairs
                - i.e. (a_i, b_i) (for all i < len(values_a)) are treated as pairs.
    Output:
        - tstats: a float, describing the t-statistics
        - p-value: a float, describing the two-tailed p-value
    """

    tstats, pvalue = ttest_rel(values_a, values_b, nan_policy='omit')
    return tstats, pvalue

if __name__ == "__main__":
    sql_path = 'final_project.db'
    conn = sqlite3.connect(sql_path)

    # dataset = ... # Some import of the data frame
    statement = 'SELECT COVID_CASE_PERCENT, PERCENT_RIDERSHIP FROM table_name'
    dataset = pd.read_sql(statement, conn)
    conn.close()

    data = dataset[['COVID_CASE_PERCENT', 'PERCENT_RIDERSHIP']]
    data = drop_incomplete_rows(data)

    cases = data[['COVID_CASE_PERCENT']]
    ridership = data[['PERCENT_RIDERSHIP']]

    tstats, pvalue = paired_ttest(cases, ridership)

    print("Test statistics: ", tstats)
    print("p-value: ", pvalue)
    print("p-value < 0.05", pvalue < 0.05)
