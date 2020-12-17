import pandas as pd
import seaborn
import numpy as np


# Loading all data sets
def load_data_sets():
    life_expectancy_df = pd.read_csv("data/Life Expectancy Data.csv")
    countries_df = pd.read_csv("data/250 Country Data.csv")
    happiness_dfs = {"2015": 0, "2016": 1, "2017": 2, "2018": 3, "2019": 4}
    for key in happiness_dfs.keys():
        happiness_dfs[key] = pd.read_csv(
            "data/Happiness_Dataset/{year}.csv".format(year=key))
    return life_expectancy_df, countries_df, happiness_dfs

# Exploring Datasets
#################################################################


def explore_life_expectancy_df(df):
    print(df[0:5])
    print(df.info())


def main():
    life_expectancy_df, countries_df, happiness_fs = load_data_sets()
    explore_life_expectancy_df(life_expectancy_df)


main()
