import requests
import json
from airflow import DAG
from datetime import datetime
from datetime import date
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import tweepy
from textblob import TextBlob
import csv

# define default args
# These args will get passed on to each operator
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 29),
    'end_date': datetime(2021, 1, 2)
}

# instantiate DAG
dag = DAG(
    'sentiment_analysis_DAG',
    default_args=default_args,
    description='Sentimental Analysis for tweets',
    schedule_interval='@daily',
)

# tweets are fetched by getting the place ids of the required countries
# then using these ids to search for the 20 most recent tweets in these countries
# tweets texts are then extracted from the returned objects
def get_tweets_callable(**kwargs):
    auth = tweepy.OAuthHandler("trdcr7BgOoqPbEszKNfI6fVSW", "2zTySVMKfJ5QC1CBo6PiIh4NZibJHXclGTalMC1N5TWHvh5BzI")
    auth.set_access_token("3341770275-7l54lG88e0pEyT6tJn35qrfNRfFqQIv0v7syfc8", "Lrt06TTUgqIhTVR4gY2L2fI7o1V1hql77Ux6yMF5EMtuj")
    api = tweepy.API(auth)
    # canada_geo_id = "3376992a082d67c7"
    # afghanistan_geo_id = "9ac7aa903ba29bd1"
    canada_geo_id = api.geo_search(query="Canada", granularity="country")[0].id 
    afghanistan_geo_id = api.geo_search(query="Afghanistan", granularity="country")[0].id
    canada_tweets = api.search(q='place:%s'%canada_geo_id, lang='en', result_type='recent', count=20, tweet_mode="extended")
    afghanistan_tweets = api.search(q='place:%s'%afghanistan_geo_id, lang='en', result_type='recent', count=20, tweet_mode="extended")
    canada_tweets_text = list(map(lambda tweet: tweet._json["full_text"], canada_tweets))
    afghanistan_tweets_text = list(map(lambda tweet: tweet._json["full_text"], afghanistan_tweets))
    return canada_tweets_text, afghanistan_tweets_text


# the sentiment of each tweet is calculated using the TextBlob library
# and each tweet is mapped to its corresponding sentiment
def calculate_sentiments_callable(**context):
    canada_tweets_text, afghanistan_tweets_text = context['task_instance'].xcom_pull(task_ids='get_tweets')
    canada_sentiments = list(map(lambda tweet: TextBlob(tweet).sentiment.polarity, canada_tweets_text))
    afghanistan_sentiments = list(map(lambda tweet: TextBlob(tweet).sentiment.polarity, afghanistan_tweets_text))
    return canada_sentiments, afghanistan_sentiments

# the average of the sentiments of the tweets of each country is calculated
# and the results are stored in a csv file along with their timestamps
def average_sentiments_callable(**context):
    canada_sentiments, afghanistan_sentiments = context['task_instance'].xcom_pull(task_ids='calculate_sentiments')
    average_canada_sentiment = sum(canada_sentiments) / len(canada_sentiments)
    average_afghanistan_sentiment = sum(afghanistan_sentiments) / len(afghanistan_sentiments)
    with open("average_sentiments.csv", "a") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["Canada", average_canada_sentiment, datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
        writer.writerow(["Afghanistan", average_afghanistan_sentiment, datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
    return average_canada_sentiment, average_afghanistan_sentiment


# the average sentiments calculated are compared with the happiness scores in the dataset
# canada's happiness score of 7.4 indicates that it is a happy country
# afghanistan's happiness score of 3.6 indicates that it is a sad country
# an average sentiment greater than 0 indicates positive (happy) tweets
# an average sentiment less than 0 indicates negative (sad) tweets
def compare_sentiments_callable(**context):
    average_canada_sentiment, average_afghanistan_sentiment = context['task_instance'].xcom_pull(task_ids='average_sentiments')
    rows = []
    with open("average_sentiments.csv") as csv_file:
        rows = list(csv.reader(csv_file))
        if float(rows[-1][1]) > 0:
            rows[-1].append("Positive Sentiment, unlike the 3.6 happiness score in the dataset which indicates a sad country")
        else:
            rows[-1].append("Negative Sentiment, aligned with the 3.6 happiness score in the dataset which indicates a sad country")
        if float(rows[-2][1]) > 0:
            rows[-2].append("Positive Sentiment, aligned with the 7.4 happiness score in the dataset which indicates a happy country")
        else:
            rows[-2].append("Negative Sentiment, unlike the 7.4 happiness score in the dataset which indicates a happy country")
    with open("average_sentiments.csv", "w") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerows(rows)


get_tweets_task = PythonOperator(
    task_id='get_tweets',
    provide_context=True,
    python_callable=get_tweets_callable,
    dag=dag,
)

calculate_sentiments_task = PythonOperator(
    task_id='calculate_sentiments',
    provide_context=True,
    python_callable=calculate_sentiments_callable,
    dag=dag,
)

average_sentiments_task = PythonOperator(
    task_id='average_sentiments',
    provide_context=True,
    python_callable=average_sentiments_callable,
    dag=dag,
)

compare_sentiments_task = PythonOperator(
    task_id='compare_sentiments',
    provide_context=True,
    python_callable=compare_sentiments_callable,
    dag=dag,
)

get_tweets_task >> calculate_sentiments_task >> average_sentiments_task >> compare_sentiments_task