import argparse
import logging
from logging.handlers import RotatingFileHandler
import os.path
import sys
import time
import traceback
import tweepy
import json
from datetime import datetime
import gzip

# configure logging
logging.captureWarnings(True)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
log_handler = logging.StreamHandler(stream=sys.stderr)
log_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s - %(message)s"))
logger.addHandler(log_handler)

log_handler = RotatingFileHandler("logs/error.log", maxBytes=100000, backupCount=10)
log_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s - %(message)s"))
logger.addHandler(log_handler)


def get_credentials(credential_file):
    # start the main program
    with open(credential_file, "rt") as f:
        credentials = json.load(f)
        return credentials

def get_accounts(account_file):
    accounts = set()
    with open(account_file, "rt") as f:
        for line in f:
            line = line.strip()
            if line and line.isdigit():
                accounts.add(line)
    return list(accounts)

def get_API(c):
    auth = tweepy.OAuthHandler(c["consumer_key"], c["consumer_secret"])
    auth.set_access_token(c["access_token"], c["access_token_secret"])
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    return api

def fetch_user_timeline(api, account_id, outputfile):
    # Only iterate through the first 3 pages
    f = gzip.open(outputfile, 'wt')
    for item in tweepy.Cursor(api.user_timeline, account_id, count=200, tweet_mode="extended").items(3200): #max is 3200
        # TODO check matching tweets
        jobj = item._json
        print(json.dumps(jobj), file=f, flush=True)
    f.close()
    return

def main(credential_file, account_file, output):
    credentials = get_credentials(credential_file)
    api = get_API(credentials)
    timestamp = "20221214" #20221214
    accounts = get_accounts(account_file)
    logger.info('total number of accounts=%s'%len(accounts))
    for account in accounts:
        output_file = os.path.join(output, "%s_%s.json.gz"%(account, timestamp))
        if os.path.exists(output_file):
            continue
        # print('output file', output_file)
        logger.info("output file: %s"%output_file)
        try:
            fetch_user_timeline(api, account, output_file)
        except Exception as e:
            logger.error("an error occurred while writing a line: {}".format(e))
            logger.error(traceback.format_exc())
        time.sleep(5)
        break
    return

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog="get_user_timeline",
        formatter_class=argparse.RawTextHelpFormatter,
        description=__doc__,
    )
    parser.add_argument("credentials", help="a json file containing credentials to use")
    parser.add_argument("accounts", help="a file containing account names")
    parser.add_argument("output", help="output directory")
    args = parser.parse_args()

    credential_file = args.credentials
    account_file = args.accounts
    output = args.output

    main(credential_file, account_file, output)
    pass