import json
from tweepy.streaming import StreamListener
from states import *
import logging
import ProcessTweetData

class TwitterStreamListener(StreamListener):
    def __init__(self, elastic, logger):
        self.es = elastic
        self.logger = logger

    def on_data(self, data):
        """
        Process tweet data from the twitter stream as it is available.
        :param data: Tweet data from twitter stream
        :return: boolean - false if something broke such as the stream connection, else true
        """
        # Clean up tweet
        try:
            tweet = json.loads(data)
        except:
            self.logger.error("Unable to parse tweet to json")

        try:
            # Determine US state
            place = self.find_place(tweet)
            if place is None:
                self.logger.info("No location for tweet  %s", tweet)
                return True

            # Add tweet json to Elasticsearch
            self.es.index(index='twitter_data', doc_type='twitter', body=tweet, ignore=400)
        except:
            self.logger.error("Unable to add tweet to Elasticsearch")
            return False

        doc = {
            "size": 10000,
            "query": {
                "id": tweet['id']
            }
        }

        res = self.es.search(index='twitter_data', doc_type='twitter', body=doc, scroll='1m')

        print(len(res))
        for document in res['hits']['hits']:
            print(document['text'])
            process = ProcessTweetData(document, self.es)
            process.process_tweet()
        return True


    def on_error(self, status):
        """If an error status is returned by the Twitter API, stop streaming tweets
        :param status: Error status from Twitter API
        :return: False to stop twitter stream
        """
        self.logger.error("Twitter API error code: %s", status)
        return False


    def find_place(self, tweet):
        """Find the location of the tweet using 'place' or 'user location' in tweets
        :param tweet: JSON tweet data to process
        :return: 2 letter state abbreviation for tweet location"""

        # Find location from place
        if tweet['place'] is not None:
            state = tweet['place']['full_name'].split(',')[1].strip()
            return state
        # Find location from user location
        elif tweet['user']['location'] is not None:
            # Split location into single word tokens
            places_splits = tweet['user']['location'].replace(',', ' ').split(' ')
            for place in places_splits:
                # Remove leading and trailing whitespaces
                place = place.strip()
                # Determine if the state abbreviation or full state name is in location
                for key, value in states.items():
                    if key.lower() == place.lower():
                        return key
                    if value.lower() == place.lower():
                        return key
            return None
        else:
            return None