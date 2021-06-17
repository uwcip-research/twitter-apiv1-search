import argparse
import logging
import sys
import traceback
import tweepy
import ujson as json
from datetime import datetime


def main():
    parser = argparse.ArgumentParser(
        prog="get_statistics",
        formatter_class=argparse.RawTextHelpFormatter,
        description=__doc__,
    )
    parser.add_argument("credentials", help="a json file containing credentials to use")
    parser.add_argument("accounts", help="a file containing account ids")
    args = parser.parse_args()

    # configure logging
    logging.captureWarnings(True)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    log_handler = logging.StreamHandler(stream=sys.stdout)
    log_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s - %(message)s"))
    logger.addHandler(log_handler)

    # start the main program
    try:
        credentials = []
        with open(args.credentials, "rt") as f:
            credentials = json.load(f)
        logger.info("found {} credentials to use".format(len(credentials)))

        apis = []
        for c in credentials:
            auth = tweepy.OAuthHandler(c["consumer_key"], c["consumer_secret"])
            auth.set_access_token(c["access_token"], c["access_token_secret"])
            api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
            apis.append((api, c["consumer_key"]))

        accounts = []
        with open(args.accounts, "rt") as f:
            for line in f:
                line = line.strip()
                if line.startswith("#"):
                    continue
                accounts.append(line)

        print("user_id,user_screen_name,captured_at,created_at,followers,friends,favorites,statuses", file=sys.stderr)
        api_key = 0
        for account in accounts:
            api = apis[api_key][0]
            api_key = api_key + 1
            if (api_key >= len(apis)):
                api_key = 0

            try:
                obj = api.get_user(id=account.strip('"').strip("'").strip())
                data = obj._json
                print(",".join([
                    data["id_str"],
                    data["screen_name"],
                    str(datetime.now()),
                    data["created_at"],
                    str(data["followers_count"]),
                    str(data["friends_count"]),
                    str(data["favourites_count"]),
                    str(data["statuses_count"]),
                ]), file=sys.stderr)
            except Exception as e:
                print("{},{}".format(account, e), file=sys.stderr)

        return 0
    except Exception:
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
