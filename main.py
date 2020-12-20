import time

from tweet import TwitterApi, TweetProcessor


def main():

    api = TwitterApi(TweetProcessor())

    results = api.proess_tweets()

    print(results)


if __name__ == "__main__":
    main()
