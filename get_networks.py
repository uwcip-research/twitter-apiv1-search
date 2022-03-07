import argparse
import logging
import json
import os
import sys
import traceback
import tweepy
from tweepy import TweepError
from datetime import datetime
from queue import Queue
from threading import Thread
from tenacity import Retrying, stop_after_attempt
from glob import glob


def main():
    parser = argparse.ArgumentParser(
        prog="get_networks",
        formatter_class=argparse.RawTextHelpFormatter,
        description=__doc__,
    )
    parser.add_argument("credentials", help="a json file containing credentials to use")
    parser.add_argument("accounts", help="a file containing account ids")
    parser.add_argument("output", help="a directory to place the output files, one per user id")
    parser.add_argument("--fetch-tweets", dest="fetch_tweets", action="store_true", help="set this flag to fetch all tweets for each account")
    parser.add_argument("--fetch-friends", dest="fetch_friends", action="store_true", help="set this flag to fetch all friends for each account")
    parser.add_argument("--fetch-followers", dest="fetch_followers", action="store_true", help="set this flag to fetch all followers for each account")
    args = parser.parse_args()

    # configure logging
    logging.captureWarnings(True)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    log_handler = logging.StreamHandler(stream=sys.stderr)
    log_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s - %(message)s"))
    logger.addHandler(log_handler)

    # start the main program
    try:
        # each file name matches an id that has been loaded
        loaded = [os.path.split(x)[1].split(".", -1)[0] for x in glob(os.path.join(args.output, "*")) if (x.endswith(".json"))]

        accounts = Queue()
        with open(args.accounts, "rt") as f:
            for line in f:
                line = line.strip()
                if line.startswith("#"):
                    continue
                if line not in loaded:
                    accounts.put(line)
                else:
                    logger.info("skipping {} because it already exists".format(line))

        credentials = []
        with open(args.credentials, "rt") as f:
            credentials = json.load(f)
        logger.info("found {} credentials to use".format(len(credentials)))

        threads = []
        for credential in credentials:
            t = Thread(args=(accounts, credential, args.output, args.fetch_tweets, args.fetch_friends, args.fetch_followers), target=process_account)
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        return 0
    except Exception:
        logger.error(traceback.format_exc())
        return 1


def process_account(queue, credentials, output, fetch_tweets, fetch_friends, fetch_followers):
    logger = logging.getLogger()

    while not queue.empty():
        account = queue.get(block=False)
        if account is None:
            continue

        try:
            logger.info("processing {} with {}".format(account, credentials["consumer_key"]))
            #for attempt in Retrying(reraise=True, stop=stop_after_attempt(5)):
            #    with attempt:
            fetch_account(account, credentials, output, fetch_tweets, fetch_friends, fetch_followers)
        except Exception as e:
            logger.error("could not get data for {}: {}".format(account, e))


def fetch_account(account, credentials, output, fetch_tweets, fetch_friends, fetch_followers):
    logger = logging.getLogger()

    api = None
    try:
        auth = tweepy.OAuthHandler(credentials["consumer_key"], credentials["consumer_secret"])
        auth.set_access_token(credentials["access_token"], credentials["access_token_secret"])
        api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    except Exception:
        logger.error("could not authenticate with consumer key {}".format(credentials["consumer_key"]))
        logger.error(traceback.format_exc())
        raise

    file_name = os.path.join(output, "{}.json.tmp".format(account))

    try:
        obj = api.get_user(id=account)
        data = obj._json
        logger.info("pulling account {}: {} tweets, {} friends, {} followers".format(
            obj.id,
            data["statuses_count"],
            data["friends_count"],
            data["followers_count"],
        ))
    except TweepError as e:
        if e.api_code == 50:
            logger.info("finished fetching unknown account {}".format(account))
            with open("{}.tmp".format(file_name), "wt") as f:
                print(json.dumps({
                    "id": account,
                    "captured_at": str(datetime.now()),
                    "unknown": True
                }, indent=4), file=f)
            os.rename("{}.tmp".format(file_name), file_name)
            return

        if e.api_code == 63:
            logger.info("finished fetching suspended account {}".format(account))
            with open("{}.tmp".format(file_name), "wt") as f:
                print(json.dumps({
                    "id": account,
                    "captured_at": str(datetime.now()),
                    "suspended": True
                }, indent=4), file=f)
            os.rename("{}.tmp".format(file_name), file_name)
            return

        raise

    if obj.protected:
        logger.info("finished fetching protected account {}".format(account))
        with open("{}.tmp".format(file_name), "wt") as f:
            print(json.dumps({
                "id": data["id_str"],
                "screen_name": data["screen_name"],
                "captured_at": str(datetime.now()),
                "created_at": str(datetime.strptime(data["created_at"], "%a %b %d %H:%M:%S +0000 %Y")),  # Sat Dec 31 04:34:35 +0000 2011
                "favorites_count": data["favourites_count"],
                "tweet_count": data["statuses_count"],
                "protected": True
            }, indent=4), file=f)
        os.rename("{}.tmp".format(file_name), file_name)
        return

    tweets = None
    if fetch_tweets:
        tweets = []
        for page in tweepy.Cursor(api.user_timeline, user_id=obj.id, stringify_ids=True, tweet_mode="extended", count=3200).pages():
            logger.info("fetching tweets page for {}".format(obj.id))
            for tweet in page:
                tweets.append(tweet._json)

    # max per page is 5000
    followers = None
    if fetch_followers:
        followers = []
        for page in tweepy.Cursor(api.followers_ids, user_id=obj.id, stringify_ids=True, count=5000).pages():
            followers += page

    # max per page is 5000
    friends = None
    if fetch_friends:
        friends = []
        for page in tweepy.Cursor(api.friends_ids, user_id=obj.id, stringify_ids=True, count=5000).pages():
            friends += page

    logger.info("finished fetching {}".format(account))
    with open(os.path.join(output, "{}.json".format(account)), "wt") as f:
        print(json.dumps({
            "id": data["id_str"],
            "screen_name": data["screen_name"],
            "captured_at": str(datetime.now()),
            "created_at": str(datetime.strptime(data["created_at"], "%a %b %d %H:%M:%S +0000 %Y")),  # Sat Dec 31 04:34:35 +0000 2011
            "favorites_count": data["favourites_count"],
            "tweet_count": data["statuses_count"],
            "friend_ids": friends,
            "follower_ids": followers,
            "tweets": tweets,
            "user": data,
        }, indent=4), file=f)

    logger.info("finished processing {}".format(account))


if __name__ == "__main__":
    sys.exit(main())
