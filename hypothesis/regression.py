import numpy as np
import pandas as pd
import sqlite3
import random
import statsmodels.api as sm
from statsmodels.tools import eval_measures
from sklearn.metrics import r2_score
from util import all_variable_names_in_df, train_test_split, RANDOM_SEED
from scipy import stats

#Goals
"""
Performs an regression test on zipcode-> (median income, caserate) to see if they are
"""

def regression(train_df, test_df, ind_var_names: list, dep_var_name: str):
    """
    Implement Linear Regression using StatsModel.

    inputs:
        - train_df: a Pandas DataFrame, containing all the training samples
        - test_df: a Pandas DataFrame, containing all the testing samples
        - ind_var_names: a list of strings of independent variable columns
                        that we want to include in the model
        - dep_var_name: the name of the dependent variable of our model
    

    outpus:
        - mse_train: the mean-squared error of the model (trained on the training
                    data), evaluated on the training dataset
        - mse_test: the mean-squared error of the model (trained on the training
                    data), evaluated on the testing dataset
        - rsquared_val: the r-squared value of the model (trained on the training
                    data), evaluated on the testing dataset
    """
    ## Stencil: Error check whether the input that you provided to the function is correct or not
    # Do not modify
    for df in [train_df, test_df]:
        assert all_variable_names_in_df(ind_var_names + [dep_var_name], df)

    # TODO: Construct X_train, X_test, y_train, y_test from train_df and test_df, where
    # X_train is a numpy array of all the independent variable instances from train_df,
    # y_train is a numpy array of all the dependent variable instances from train_df,
    # and the same applies to X_test and y_test from test_df.
    # Hint: Look up (1) how to select a Pandas DataFrame B with a subset of columns from a given DataFrame A,
    #           and (2) how to use Pandas .to_numpy() function.
    
    x_train = train_df[ind_var_names].to_numpy()
    x_test = test_df[ind_var_names].to_numpy()
    y_train = train_df[dep_var_name].to_numpy()
    y_test = test_df[dep_var_name].to_numpy()

    x_train = sm.add_constant(x_train)
    x_test = sm.add_constant(x_test)

    # TODO: Using statsmodel, fit a linear regression model to the training dataset
    # You may checkout statsmodel's documentation here: https://www.statsmodels.org/stable/regression.html
    
    train_model = sm.OLS(y_train, x_train).fit()
    y_train_pred = train_model.predict(x_train)

    # TODO: Using statsmodel's eval_measures MSE calculation function,
    # calculate the Mean-squared Error of the model above (on the training dataset)

    mse_train_a = eval_measures.mse(y_train, y_train_pred)

    # TODO: Similarly, calculate the Mean-squared Error of the model above (on the testing dataset)
    
    test_model = sm.OLS(y_train, x_train).fit()
    y_test_pred = train_model.predict(x_test)

    mse_test_a = eval_measures.mse(y_test, y_test_pred)

    # TODO: Calculate the *test* R-squared value (using sklearn's r2_score function)
    
    rsquared_val_a = r2_score(y_test, y_test_pred)

    # TODO: Print out the summary to see more information as needed

    print(train_model.summary())
    print(test_model.summary())
    print('------------------------------')
    # slope, intercept, r_value, p_value, std_err = stats.linregress(x_train,y_train)

    # TODO: Replace these values with whatever you found!
    mse_train, mse_test, rsquared_val = mse_train_a, mse_test_a, rsquared_val_a
    
    # And return them! :)
    return mse_train, mse_test, rsquared_val
    

def main():
    # TODO: Load the data from the bike-sharing.csv file into a Pandas DataFrame. Do not change
    # the variable name /data/

    sql_path = 'final_project.db'
    conn = sqlite3.connect(sql_path)

    statement = 'SELECT COVID_CASE_PERCENT, MED_INCOME FROM demo_covid_table'
    data = pd.read_sql(statement, conn)
    conn.close()
    
    # TODO: Uncomment this to print out the column names of data to know what features there are 
    # in the dataset
    # print("Columns: ", data.columns)

    # TODO: Modify the list IND_VAR_NAMES to select the independent variables you want to perform
    # a linear regression on
    IND_VAR_NAMES = ['COVID_CASE_PERCENT']

    # Select the dependent variable name DEP_VAR_NAME
    DEP_VAR_NAME = "MED_INCOME"

    # TODO: Using the imported train_test_split function (from util.py), create the train_df and
    # test_df that will be passed into regression.
    train_df, test_df = train_test_split(data)# None, None

    # TODO: Call regression and perform other calculations as you deem necessary to answer the
    # questions posed for this section.
    mse_train, mse_test, rsquared_val = regression(train_df, test_df, IND_VAR_NAMES, DEP_VAR_NAME)
    print(mse_train, mse_test, rsquared_val)



############ DON'T MODIFY BELOW THIS LINE ############

if __name__ == "__main__":
    np.random.seed(RANDOM_SEED)
    random.seed(RANDOM_SEED)
    main()