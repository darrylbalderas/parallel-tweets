from multiprocessing import Process, Queue
import os
import tweepy


class TweetProcessor:
    def __init__(self, num_workers=5) -> None:
        self.tweets = Queue()
        self._results = Queue()
        self.num_workers = num_workers
        self.processes = [
            Process(
                target=self.process_tweets,
                args=(),
            ) for _ in range(self.num_workers)
        ]

    @property
    def results(self):
        return self._results

    def add_tweet(self, tweet) -> None:
        self.tweets.put(tweet)

    def process_tweets(self) -> None:
        while not self.tweets.empty():
            tweet = self.tweets.get()

            self._results.put(
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


class TwitterApi:
    def __init__(self, tweet_processor: TweetProcessor) -> None:
        consumer_key = os.environ.get("CONSUMER_KEY")
        consumer_secret = os.environ.get("CONSUMER_SECRET")
        access_token = os.environ.get("ACCESS_TOKEN")
        access_token_secret = os.environ.get("ACCESS_TOKEN_SECRET")
        # Authenticate to Twitter
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        # Create API object
        self.api = tweepy.API(auth)
        self.tweet_processor = tweet_processor
        self.acceptable_countries = {'US'}

    def proess_tweets(self, acceptable_countries={'US'}, search_size=10, num_topics=3):
        for country_trend in self.api.trends_available():
            # TODO: Filter based on country and state/province
            if country_trend['countryCode'] not in acceptable_countries:
                continue

            woeid = country_trend['woeid']

            # TODO: Get California trending topics every 5 minutes
            trending_topics = sorted(self.api.trends_place(woeid)[0]['trends'],
                                     key=lambda x: x['tweet_volume']
                                     if x['tweet_volume'] else float('-inf'),
                                     reverse=True)
            # Sort by tweet_volume # get 3 top trends
            for topic in trending_topics[:num_topics]:
                for tweet in tweepy.Cursor(self.api.search,
                                           q=topic['name'],
                                           result_type='recent').items(search_size):
                    self.tweet_processor.add_tweet(tweet)

        self.tweet_processor.run()
        return self.tweet_processor.results
