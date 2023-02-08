import argparse
import logging
import sys
import traceback
import tweepy
import json
import os
from queue import Queue
from tweepy import TweepError
from threading import Thread
import gzip

# configure logging
logging.captureWarnings(True)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
log_handler = logging.StreamHandler(stream=sys.stderr)
log_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s - %(message)s"))
logger.addHandler(log_handler)

def get_ids(input):
    tweet_ids = []
    with open(input, "rt") as f:
        for line in f:
            line = line.strip()
            tweet_ids.append(line)
    return tweet_ids

def get_one_batch(tweet_ids, credentials, output):
    try:
        auth = tweepy.OAuthHandler(credentials["consumer_key"], credentials["consumer_secret"])
        auth.set_access_token(credentials["access_token"], credentials["access_token_secret"])
        api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

        write_file = os.path.join(output, "{}.json.gz".format(tweet_ids[0]))
        if os.path.exists(write_file):
            logger.info("data already fetched {}".format(write_file))
            return

        results = []
        tweets = api.statuses_lookup(tweet_ids, tweet_mode="extended")
        for tweet in tweets:
            results.append(tweet._json)

        with gzip.open(write_file, 'wt') as f:
            for tweet in results:
                f.write(json.dumps(tweet, default=str, ensure_ascii=False))
                f.write("\n")

    except Exception:
        logger.error("could not authenticate with consumer key {}".format(credentials["consumer_key"]))
        logger.error(traceback.format_exc())
        raise

    return

def get_tweets(queue, credentials, output):
    logger = logging.getLogger()

    while not queue.empty():
        tweet_ids = queue.get(block=False)
        if tweet_ids is None:
            continue
        try:
            logger.info("processing {} with {}".format(tweet_ids[:5], credentials["consumer_key"]))
            get_one_batch(tweet_ids, credentials, output)
        except Exception as e:
            logger.error("could not get data for {}: {}".format(tweet_ids[:5], e))
    return

def split(list_a, chunk_size=100):
  for i in range(0, len(list_a), chunk_size):
    yield tuple(list_a[i:i + chunk_size])

def batch_get_tweets(credential_file, input, output):
    credentials = []
    with open(credential_file, "rt") as f:
        credentials = json.load(f)
    logger.info("found {} credentials to use".format(len(credentials)))

    tweet_ids = get_ids(input)
    logger.info("total number of {} tweet ids".format(len(tweet_ids)))

    tweet_ids_chunks = list(split(tweet_ids, 100))
    logger.info("total number of {} chunks".format(len(tweet_ids_chunks)))

    queue = Queue()
    for chunk in tweet_ids_chunks:
        queue.put(chunk)

    threads = []
    for credential in credentials:
        t = Thread(args=(queue, credential, output), target=get_tweets)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    return

def main():
    parser = argparse.ArgumentParser(
        prog="get_networks",
        formatter_class=argparse.RawTextHelpFormatter,
        description=__doc__,
    )
    parser.add_argument("credentials", help="a json file containing credentials to use")
    parser.add_argument("input", help="a file containing all tweet ids")
    parser.add_argument("output", help="a directory to place the output files, one per user id")
    args = parser.parse_args()

    batch_get_tweets(args.credentials, args.input, args.output)
    return

if __name__ == '__main__':
    main()