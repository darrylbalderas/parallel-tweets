from multiprocessing import Process, Queue
import os
import tweepy


class TweetProcessor:
    def __init__(self, num_workers=4) -> None:
        self.tweets = Queue()
        self.results = Queue()
        self.num_workers = num_workers

    def add_tweet(self, tweet) -> None:
        self.tweets.put(tweet)

    def transform(self) -> None:
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

    def run(self):
        processes = [
            Process(
                target=self.transform,
                args=(),
            ) for _ in range(self.num_workers)
        ]

        for p in processes:
            p.start()

        for p in processes:
            p.join()

    def output(self):
        results = []
        while not self.results.empty():
            results.append(self.results.get())
        return results


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
        self.acceptable_towns = {'San Francisco'}

    def apply_trend_filter(self, trend):
        country_code = trend['countryCode']
        town_name = trend['name']
        is_country_code = country_code in self.acceptable_countries
        is_town = town_name in self.acceptable_towns
        return is_country_code and is_town

    def proess_tweets(self, search_size=10, num_topics=5):
        country_trends = filter(self.apply_trend_filter, self.api.trends_available())
        for country_trend in country_trends:
            woeid = country_trend['woeid']
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
        return self.tweet_processor.output()
