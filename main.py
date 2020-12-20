import os

import tweepy


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
