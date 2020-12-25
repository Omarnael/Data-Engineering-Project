import sklearn
import numpy as np
import pandas as pd
import requests

# determine Economic status according to gdp value
def gdp_category(x):
    if  1 >= x >= 0.63:
      return "Rich"
    elif 0.63 >= x >= 0.25:
      return "Medium"
    else:
      return "Poor"

# determine country category, east or west according to list of eastern countries stored
def country_category(country, response):
    for line in response.content.splitlines():
        if country == line.decode("utf-8"):
            return "Eastern"
    return "Western"

# add economic status feature by applying the gdp_category function
def add_economic_status_column(integrated_df):
    integrated_df['Economic Status'] = integrated_df['GDP/Capita_normalized'].apply(gdp_category)
    return integrated_df

# add country category feature by applying the country_category function
def add_country_category_column(integrated_df):
    response = requests.get("https://raw.githubusercontent.com/PhilipMouris/country_data/main/eastern_countries.txt")
    integrated_df['Country Category'] = integrated_df['Country'].apply(country_category, response=response)
    return integrated_df

# add quality of life feature by calculating product of life expectancy and happiness score
def add_quality_of_life_column(integrated_df):
    integrated_df["Quality Of Life"] = (integrated_df["Life Expectancy_normalized"] * integrated_df["Happiness Score_normalized"])
    return integrated_df

# add democratic feature by checking value of freedom, threshold is mean
def add_democratic_column(integrated_df):
    integrated_df['Democratic?'] = integrated_df["Freedom"] >= 0.438226
    return integrated_df

def engineer_features(integrated_df):
    integrated_df = add_economic_status_column(integrated_df)
    integrated_df = add_country_category_column(integrated_df)
    integrated_df = add_quality_of_life_column(integrated_df)
    integrated_df = add_democratic_column(integrated_df)
    return integrated_df