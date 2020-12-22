from tweet import TwitterProcessor
import tweepy
import os


def main():
    consumer_key = os.environ.get("CONSUMER_KEY")
    consumer_secret = os.environ.get("CONSUMER_SECRET")
    access_token = os.environ.get("ACCESS_TOKEN")
    access_token_secret = os.environ.get("ACCESS_TOKEN_SECRET")
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    processor = TwitterProcessor(tweepy.API(auth))

    processor.pull_trending_tweets()

    results = processor.get_results()
    # TODO: Store results in Dynamo DB
    print(results)


if __name__ == "__main__":
    main()
