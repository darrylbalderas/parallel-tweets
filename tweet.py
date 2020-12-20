from multiprocessing import Process, Queue
import random


def gather_tweets(tweets: Queue):
    tweets.put(f"{round(random.random(), 3)}")


def process_tweets(tweets: Queue, result: Queue):
    if not tweets.empty():
        tweet = tweets.get()
        result.put(f"prefix {tweet}")


# TODO: Create class that handles your custom api calls (http://docs.tweepy.org/en/latest/api.html#TweepError)

# TODO: Create rate limit wrapper to avoid hitting rate limits (http://docs.tweepy.org/en/latest/api.html#RateLimitError)