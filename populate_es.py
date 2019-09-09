from tweepy import OAuthHandler
from tweepy import Stream
from elasticsearch import Elasticsearch
from config import *
from mapping import *
from TwitterStreamListener import TwitterStreamListener
import logging
import logging.config

if __name__ == '__main__':
	"""Main script to execute:
		Initialise and stream tweets
	"""
	# logging.basicConfig(filename='/home/twitter/log.log', level=logging.DEBUG)

	# logging.config.fileConfig('logging.conf')
	logger = logging.getLogger('simpleExample')

	# Create TwitterStreamListener instance and connect to Elasticsearch
	es = Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])
	listener = TwitterStreamListener(es, logger)

	print(consumer_key, consumer_secret, access_token, access_token_secret)
	# Authorise twitter
	auth = OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_token_secret)

	# If the twitter_data index does not exist in Elasticsearch, create the index
	if not es.indices.exists(index='twitter_data'):
		logger.info('No index twitter_data')
		request_body = {
				"settings": {
					"number_of_shards": 1,
					"number_of_replicas": 0
					}
				}
		es.indices.create(index='twitter_data', body=request_body)
		es.indices.put_mapping(index='twitter_data', doc_type='twitter', body=mapping)

	while True:
		try:
			stream = Stream(auth, listener, tweet_mode='extended')
			stream.filter(track=['@earthquake', '#rescue', '#alaska', '#help'])
		except Exception as excpt:
			print(excpt)
			logger.error("something happened here")
