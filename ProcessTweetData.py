import datetime
import json
import logging
import re
from textblob import TextBlob
from textblob.sentiments import NaiveBayesAnalyzer


from states import *


class ProcessTweetData():
    def __init__(self, tweet, elastic, logger):
        self.tweet = tweet
        self.es = elastic
        self.logger = logger

    def process_tweet(self):
        try:
            cleantweet = self.clean_tweet(self.tweet['text'])
        except:
            self.logger.error("Unable to parse tweet to json")
            return True

        # Do sentiment analysis
        sentiment_textblob = self.analyse_sentiment_textblob(cleantweet)
        sentiment_textblob_nb = self.analyse_sentiment_textblob_nb(cleantweet)

        # Format date
        date_created = str(self.tweet['created_at'])
        formatted_date = datetime.strptime(date_created, '%a %b %d %H:%M:%S %z %Y')

        body = {"doc": {"state": place, "state_name": states[place], "sentiment_tb": sentiment_textblob, "sentiment_tbnb": sentiment_textblob_nb, "date": formatted_date}}
        self.es.update(index='twitter_data', doc_type='twitter', id=self.tweet.id, body=body)

    def clean_tweet(self, tweet):
        """Clean up tweet text by removing user mentions and urls
        :param tweet: tweet text as string
        :return: cleaned tweet text as string"""
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/S+)|(http\S+)", " ", tweet).split())

    def analyse_sentiment_textblob(self, tweet):
        """Determine sentiment using TextBlob PatterAnalyzer
        :param tweet: tweet text as string
        :return: Tweet sentiment value (determined from PatternAnalyzer polarity)"""
        analysis = TextBlob(tweet)
        if analysis.sentiment.polarity > 0:
            return 1
        elif analysis.sentiment.polarity == 0:
            return 0
        else:
            return -1

    def analyse_sentiment_textblob_nb(self, tweet):
        """Determine sentiment using TextBlob NaiveBayesAnalyzer
        :param tweet: tweet text as string
        :return: tweet sentiment value (determined from NaiveBayes classification)"""
        analysis = TextBlob(tweet, analyzer=NaiveBayesAnalyzer())
        if analysis.sentiment.classification == 'pos':
            return 1
        else:
            return -1
