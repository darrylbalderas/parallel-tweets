from multiprocessing import Process, Queue, process
import tweepy


class TwitterProcessor:
    def __init__(self, api: tweepy.API, num_workers=5) -> None:
        self.api = api
        self.acceptable_countries = {'US'}
        self.acceptable_towns = {'San Francisco'}
        self.tweets = Queue()
        self.results = Queue()
        self.num_workers = num_workers
        self.search_size_per_topic = 2

    def apply_trend_filter(self, trend):
        country_code = trend['countryCode']
        town_name = trend['name']
        is_country_code = country_code in self.acceptable_countries
        is_town = town_name in self.acceptable_towns
        return is_country_code and is_town

    def pull_trending_tweets(self):
        country_trends = filter(self.apply_trend_filter, self.api.trends_available())
        topics = Queue()

        for country_trend in country_trends:
            woeid = country_trend['woeid']
            for topic in self.api.trends_place(woeid)[0]['trends']:
                topics.put(topic)

        processes = []
        for _ in range(self.num_workers):
            processes.append(Process(
                target=self.store_tweets,
                args=(topics, ),
            ))
        handle_process(processes)

    def store_tweets(self, topics) -> None:
        while not topics.empty():
            topic = topics.get()
            updated_topic = topic['name'].replace('#', '').replace(' ', '_')
            for tweet in self.api.search(q=topic['name'],
                                         result_type='recent',
                                         count=self.search_size_per_topic):
                updated_tweet = dict(text=tweet.text,
                                     id=tweet.id,
                                     created_id=tweet.created_at,
                                     retweet_count=tweet.retweet_count,
                                     lang=tweet.lang,
                                     topic=updated_topic)
                self.tweets.put(updated_tweet)

    def process_tweets(self) -> None:
        # TODO: Use celery to run a background task every time tweets get queued up
        while not self.tweets.empty():
            tweet = self.tweets.get()
            self.results.put(tweet)

    def get_results(self):
        # TODO: swap with celery background task
        processes = [
            Process(
                target=self.process_tweets,
                args=(),
            ) for _ in range(self.num_workers)
        ]

        handle_process(processes)

        results = []
        while not self.results.empty():
            results.append(self.results.get())
        return results


def handle_process(processes):
    for p in processes:
        p.start()

    while any([p.is_alive() for p in processes]):
        pass

    for p in processes:
        p.join()
