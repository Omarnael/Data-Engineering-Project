# CSEN1095-W20-Project

Google Colab: https://colab.research.google.com/drive/127GOoYIaBo1eAHg2BKPFFtSaL9e_lx-9?usp=sharing

## Datasets

**World Happiness Report:** This dataset contains data about the happiness scores and rankings of all countries based on The World Happiness Report which is a landmark survey of the state of global happiness.

**Life Expectancy (WHO):** This dataset contains data about life expectancy and other health factors, based on data provided by the WHO

**All 250 Country Data:** This dataset contains information about all countries

## Motivation and Goals

The goal of this project is to answer some questions regarding the topics covered by the datasets, by going through the complete data engineering process.

## Process
 The datasets were extracted, transformed, integrated and analyzed to answer the research questions. Exact details about the process can be found in the notebook.
 
## Research Questions
1- Live happier, Live longer!\
2- West Vs East: A battle of happiness\
3- What is the relationship between happiness and BMI?\
4- Can money truely buy happiness?\
5- Regions with the highest number of measles\
6- How is wealth distributed in developed and developing countries?\
7- How is wealth distributed in eastern and westerns countries?\
8- How are Literacy rate and life expectancy related?\
9- Do regions with higher unemployement and literacy rates have a higher number of HIV infections ? (More drug usage(sharing needles, higher sexual activity etc.)\
10- Trends of quality of life over the period of 2015-2019 based on region of the country.\
11- Quality of Life calculated as Life Expectancy * Happiness Score vs Economic status (Rich,Medium,Poor)

## Features Added
**Economic Status (Indicator):** This feature was engineered by categorizing countries into Poor, Medium and Rich countries according to GDP/Capita_normalized.\
**Country Category (Indicator):** This feature was engineered by categorizing countries into Western and Eastern countries.\
**Quality of life (Interaction):** This feature was engineered by calculating the product of Life Expectancy_normalized and Happiness Score_normalized.\
**Democratic? (Indicator):** This feature is an indicator to whether a country is democratic or not. It is engineered by checking the mean of the Freedom column for all countries. If a country has a freedom value higher than the mean, then it is a democratic country, otherwise it is not.

## Pipeline
A pipeline was implemented using Airflow. The pipeline consists of the following tasks:

**Load_Data_Sets:** This task loads the datasets from a shared repo to which we uploaded the datasets.\
**Normalize_Column_Names:** This task noramalizes column names of all datasets to have the same format.\
**Transform_Life_Expectancy:** This task applies data cleaning to the life expectancy dataset.\
**Transform_Happiness:** This task applies data cleaning to the happiness dataset.\
**Transform_Countries:** This task applies data cleaning to the countries dataset.\
**Integration:** This task integrates the 3 datasets into 1 final dataset.\
**Feature_Engineering:** This task adds new engineered features to the dataset.\
**Store:** This task stores the final dataset as a csv file.
