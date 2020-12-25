import sklearn
from utils.general import iterative_impute
from sklearn.impute import IterativeImputer
import numpy as np

# adjust format of column names
def rename_columns(df):
    df = df.rename(str.strip, axis='columns')
    df = df.rename(columns=lambda name:  " ".join(
        w[:1].upper() + w[1:] for w in name.split()))
    return df

# renaming column names to make them more descriptive or compact
def adjust_column_names(happiness_dfs):
    happiness_dfs["2017"].rename(columns={"Happiness.Rank": "Happiness Rank", "Happiness.Score": "Happiness Score",
                                          "Economy..GDP.per.Capita.": "Economy (GDP per Capita)",
                                          "Health..Life.Expectancy.": "Health (Life Expectancy)",
                                          "Trust..Government.Corruption.": "Trust (Government Corruption)",
                                          "Dystopia.Residual": "Dystopia Residual"}, inplace=True)
    happiness_dfs["2018"].rename(columns={"Score": "Happiness Score",
                                          "GDP Per Capita": "Economy (GDP per Capita)",
                                          "Healthy Life Expectancy": "Health (Life Expectancy)",
                                          "Perceptions Of Corruption": "Trust (Government Corruption)",
                                          "Freedom To Make Life Choices": "Freedom",
                                          "Overall Rank": "Happiness Rank",
                                          "Social Support": "Family"}, inplace=True)
    happiness_dfs["2019"].rename(columns={"Score": "Happiness Score",
                                          "GDP Per Capita": "Economy (GDP per Capita)",
                                          "Healthy Life Expectancy": "Health (Life Expectancy)",
                                          "Perceptions Of Corruption": "Trust (Government Corruption)",
                                          "Freedom To Make Life Choices": "Freedom",
                                          "Overall Rank": "Happiness Rank",
                                          "Social Support": "Family"}, inplace=True)

    return happiness_dfs

# to achieve consitency across the datasets, a new column was created Location. 
# This column contains the country or region of each entry, giving the priority 
# to the country if both country and region values are available.
def adjust_location(happiness_dfs):
    for key in happiness_dfs.keys():
        if "Country" in happiness_dfs[key]:
            happiness_dfs[key]["Location"] = happiness_dfs[key]["Country"]
            happiness_dfs[key].drop(['Country', 'Region'],
                                    inplace=True, axis=1, errors='ignore')
        elif "Country Or Region" in happiness_dfs[key]:
            happiness_dfs[key]["Location"] = happiness_dfs[key]["Country Or Region"]
            happiness_dfs[key].drop('Country Or Region', inplace=True, axis=1)
    return happiness_dfs

# merge all datasets of all years into one dataset
def merge_happiness_dfs(happiness_dfs):
    happiness_dfs_list = []
    for key in happiness_dfs.keys():
        happiness_dfs[key]['Year'] = key
        happiness_dfs[key] = rename_columns(happiness_dfs[key])
        happiness_dfs_list.append(happiness_dfs[key])
    happiness_dfs_merged = happiness_dfs_list[0]
    for happines_df in happiness_dfs_list:
        happiness_dfs_merged = happiness_dfs_merged.merge(
            happines_df, how="outer")
    return happiness_dfs_merged

# drop columns that are not needed
def drop_unused_columns(happiness_dfs_merged):
    happiness_dfs_merged.drop(columns=['Lower Confidence Interval', 'Upper Confidence Interval',
                                       'Whisker.high', 'Whisker.low', 'Standard Error'], inplace=True)
    return happiness_dfs_merged

# impute missing Dystopia Residual values using Health, Freedom, Trust and Generosity values
def compute_dystopia(happiness_dfs_merged):
    happiness_dfs_merged['Dystopia Residual'].fillna(happiness_dfs_merged['Health (Life Expectancy)']
                                                     + happiness_dfs_merged['Freedom']
                                                     + happiness_dfs_merged['Trust (Government Corruption)']
                                                     + happiness_dfs_merged['Generosity'], inplace=True)
    return happiness_dfs_merged

# drop entries with NA values
def drop_na_columns(happiness_dfs_merged):
    happiness_dfs_merged.dropna(inplace=True)
    return happiness_dfs_merged

# An exponential moving average is used to smooth out the noise 
# (since this is a time series). The idea behind the average is 
# to take into consideration closer times more than others. However, 
# this method is applied only by grouping the locations and not the entire column
def smoothe_noise(happiness_dfs_merged):
    for column in happiness_dfs_merged.select_dtypes(exclude="object").columns:
        if(column == "Year"):
            continue
    smoothed_values = happiness_dfs_merged.groupby('Location')[column].transform(
        lambda x: x.ewm(span=40, adjust=False).mean())
    happiness_dfs_merged[column] = smoothed_values
    return happiness_dfs_merged


def transform_happiness(happiness_dfs):
    happiness_dfs = adjust_column_names(happiness_dfs)
    happiness_dfs = adjust_location(happiness_dfs)
    happiness_dfs = merge_happiness_dfs(happiness_dfs)
    happiness_dfs = drop_unused_columns(happiness_dfs)
    happiness_dfs = compute_dystopia(happiness_dfs)
    happiness_dfs = drop_na_columns(happiness_dfs)
    happiness_dfs = smoothe_noise(happiness_dfs)
    return happiness_dfs
