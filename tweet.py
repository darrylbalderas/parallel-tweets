from multiprocessing import Process, Queue
import tweepy


class TweetProcessor:
    def __init__(self, api, num_workers=4) -> None:
        self.tweets = Queue()
        self.results = Queue()
        self.num_workers = num_workers
        self.search_size_per_topic = 10
        self.api = api


class TwitterProcessor:
    def __init__(self, api: tweepy.API, num_workers=3) -> None:
        self.api = api
        self.acceptable_countries = {'US'}
        self.acceptable_towns = {'San Francisco'}
        self.tweets = Queue()
        self.results = Queue()
        self.num_workers = num_workers
        self.search_size_per_topic = 10

    def apply_trend_filter(self, trend):
        country_code = trend['countryCode']
        town_name = trend['name']
        is_country_code = country_code in self.acceptable_countries
        is_town = town_name in self.acceptable_towns
        return is_country_code and is_town

    def pull_trending_tweets(self, search_size=10, num_topics=5):
        country_trends = filter(self.apply_trend_filter, self.api.trends_available())
        for country_trend in country_trends:
            woeid = country_trend['woeid']
            trending_topics = sorted(self.api.trends_place(woeid)[0]['trends'],
                                     key=lambda x: x['tweet_volume']
                                     if x['tweet_volume'] else float('-inf'),
                                     reverse=True)
            processes = []
            slices = len(trending_topics) // self.num_workers
            for i in range(self.num_workers):
                start = i * slices
                end = i * slices + slices
                delta = abs(end - len(trending_topics))
                if delta < slices:
                    end += delta
                processes.append(
                    Process(
                        target=self.store_tweets,
                        args=(trending_topics, start, end),
                    ))

            for p in processes:
                p.start()

            for p in processes:
                p.join()

    def store_tweets(self, topics, start, end) -> None:
        for topic in topics[start:end]:
            for tweet in self.api.search(q=topic['name'],
                                         result_type='recent',
                                         count=10):
                # for tweet in tweepy.Cursor(self.api.search,
                #                            q=topic['name'],
                #                            result_type='recent').items(
                #                                self.search_size_per_topic):
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

    def get_results(self):
        processes = [
            Process(
                target=self.process_tweets,
                args=(),
            ) for _ in range(self.num_workers)
        ]

        for p in processes:
            p.start()

        for p in processes:
            p.join()

        results = []
        while not self.results.empty():
            results.append(self.results.get())
        return results
