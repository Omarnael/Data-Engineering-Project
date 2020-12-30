# Milestone 3
An airflow sentimental analysis pipeline for 2 countries, Canada (happy) and Afghanistan (sad).
## Steps
**Get_Tweets:** This task gets the 20 most recent tweets for each of Canada and Afghanistan.\
**Calculate_Sentiments:** This task calculates a sentiment score for each tweet of the fetched tweets.\
**Average_Sentiments:** This task calculates the average sentiment score for all tweets of each country, and stores the result in a csv file.\
**Compare_Sentiments:** This task compares the average sentiment scores for each country with their happiness scores in the dataset, and stores the comparison in the csv file.\
