from multiprocessing import Process, Queue
import random


def gather_tweets(tweets: Queue):
    tweets.put(f"{round(random.random(), 3)}")


def process_tweets(tweets: Queue, result: Queue):
    if not tweets.empty():
        tweet = tweets.get()
        result.put(f"prefix {tweet}")


# TODO: Create class that handles your custom api calls (http://docs.tweepy.org/en/latest/api.html#TweepError)


class TweetProcessor:
    def __init__(self, num_workers=5) -> None:
        self.tweets = Queue()
        self.results = Queue()
        self.num_workers = num_workers
        self.processes = [
            Process(
                target=self.process_tweets,
                args=(),
            ) for _ in range(self.num_workers)
        ]

    def add_tweet(self, tweet) -> None:
        self.tweets.put(tweet)

    def process_tweets(self) -> None:
        while not self.tweets.empty():
            tweet = self.tweets.get()

            self.results.put(
                dict(text=tweet.text,
                     id=tweet.id,
                     created_id=tweet.created_at,
                     retweet_count=tweet.retweet_count,
                     lang=tweet.lang,
                     user_id=tweet.user.id,
                     user_screen_name=tweet.user.screen_name))

    def invoke_workers(self):
        for p in self.processes:
            p.start()

        for p in self.processes:
            p.join()

    def run(self):
        self.invoke_workers()
