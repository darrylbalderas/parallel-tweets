from multiprocessing import Process, Queue
import os

import tweepy

from tweet import gather_tweets, process_tweets


def parallel_job():
    tweets = Queue()
    results = Queue()
    num_workers = 5

    gather_processes = [
        Process(target=gather_tweets, args=(tweets, )) for _ in range(num_workers)
    ]

    tweet_processes = [
        Process(target=process_tweets, args=(
            tweets,
            results,
        )) for _ in range(num_workers)
    ]

    for p in gather_processes:
        p.start()

    for p in gather_processes:
        p.join()

    for p in tweet_processes:
        p.start()

    for p in tweet_processes:
        p.join()

    while not results.empty():
        print(results.get())


def main():

    consumer_key = os.environ.get("CONSUMER_KEY")
    consumer_secret = os.environ.get("CONSUMER_SECRET")
    access_token = os.environ.get("ACCESS_TOKEN")
    access_token_secret = os.environ.get("ACCESS_TOKEN_SECRET")

    # Authenticate to Twitter
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    # Create API object
    api = tweepy.API(auth)
    # TODO:  # get trends happening in country code: US
    trends = api.trends_available()  # get trends happening in country code: US
    acceptable_countries = set(['US'])

    for t in trends:
        if t['countryCode'] in acceptable_countries:
            woeid = t['woeid']
            trending_topics = sorted(api.trends_place(woeid)['trends'],
                                     key=lambda x: x['tweet_volume'],
                                     reverse=True)
            for topic in trending_topics[:3]:  # Sort by tweet_volume # get 3 top trends
                search_size = 10
                for search in tweepy.Cursor(api.search,
                                            q=topic.name,
                                            result_type='recent').items(search_size):
                    print(search.text, search.id, search.created_at,
                          search.retweet_count, search.lang, search.user.id,
                          search.user.screen_name)


if __name__ == "__main__":
    main()
