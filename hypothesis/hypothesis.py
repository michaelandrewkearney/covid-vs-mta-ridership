import numpy as np
import pandas as pd
import sqlite3
from scipy.stats import ttest_rel
import matplotlib.pyplot as plt
plt.style.use('seaborn-whitegrid')
from sklearn.model_selection import train_test_split

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
    #statement = 'SELECT COVID_CASE_PERCENT, PERCENT_RIDERSHIP FROM table_name'
    statement = 'SELECT CASERATE, PER_DIF FROM proccessed_table'
    dataset = pd.read_sql(statement, conn)
    conn.close()

    data = dataset[['CASERATE', 'PER_DIF']]
    #data = drop_incomplete_rows(data)

    train_df, test_df = train_test_split(data)

    cases_train = train_df[['CASERATE']]
    ridership_train = train_df[['PER_DIF']]

    cases_test = test_df[['CASERATE']]
    ridership_test = test_df[['PER_DIF']]

    #plt.scatter(cases, ridership, alpha=0.2);
    #'o', markersize=1);
    #plt.show()

    tstats, pvalue = paired_ttest(cases_train, ridership_train)
    print("")

    print("Test statistics: ", tstats)
    print("p-value: ", pvalue)
    print("p-value < 0.05", pvalue < 0.05)

    tstats, pvalue = paired_ttest(cases_test, ridership_test)
    print("")

    print("Test statistics: ", tstats)
    print("p-value: ", pvalue)
    print("p-value < 0.05", pvalue < 0.05)

