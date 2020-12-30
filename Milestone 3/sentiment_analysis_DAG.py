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

get_tweets_task = PythonOperator(
    task_id='get_tweets',
    provide_context=True,
    python_callable=get_tweets_callable,
    dag=dag,
)