import argparse
import csv
import getpass
import os
import psycopg2
import re
import sys
import traceback
import ujson as json
from collections import OrderedDict
from glob import glob
from html.parser import HTMLParser
from io import TextIOBase, StringIO


# compile this for performance later in the module
NULL_TERMINATOR = re.compile(r"(?<!\\)\\u0000")


def replace_null_terminators(text: str, replacement: str = r""):
    return NULL_TERMINATOR.sub(replacement, text) if text is not None else None


def get_source(data):
    parser = HTMLTextExtractor()
    parser.feed(data)
    return parser.text


def get_complete_text(tweet):
    tweet_complete_text = tweet["full_text"]
    if tweet["truncated"]:
        tweet_complete_text = tweet["extended_tweet"]["full_text"]

    if "retweeted_status" in tweet:
        return_text = "RT @{username}: {orig_complete_text}".format(
            username=tweet["retweeted_status"]["user"]["screen_name"],
            orig_complete_text=get_complete_text(tweet["retweeted_status"]))
        return return_text

    elif "quoted_status" in tweet:
        return_text = "{qt_complete_text} QT @{username}: {orig_complete_text}".format(
            qt_complete_text=tweet_complete_text,
            username=tweet["quoted_status"]["user"]["screen_name"],
            orig_complete_text=get_complete_text(tweet["quoted_status"]))
        return return_text

    else:
        return tweet_complete_text


class HTMLTextExtractor(HTMLParser):
    text = ""

    def handle_data(self, data):
        self.text = "{} {}".format(self.text, data).strip()


# this function was taken from here: https://hakibenita.com/fast-load-data-python-postgresql
class StringIteratorIO(TextIOBase):
    def __init__(self, i):
        self.i = i
        self.buffer = ""

    def readable(self) -> bool:
        return True

    def _read_one(self, n: int = None) -> str:
        while not self.buffer:
            try:
                self.buffer = next(self.i)
            except StopIteration:
                break
        ret = self.buffer[:n]
        self.buffer = self.buffer[len(ret):]
        return ret

    def read(self, n: int = None) -> str:
        line = []

        if n is None or n < 0:
            while True:
                # read the entire thing
                m = self._read_one()
                if not m:
                    break
                line.append(m)
        else:
            while n > 0:
                # read some portion
                m = self._read_one(n)
                if not m:
                    break
                n -= len(m)
                line.append(m)

        return "".join(line)


def main(**kwargs):
    if os.path.isfile(kwargs["input"]):
        load(kwargs["host"], kwargs["database"], kwargs["username"], kwargs["prefix"], kwargs["input"])
    else:
        input_files = [x for x in glob(kwargs["input"]) if (x.endswith(".json") or x.endswith(".json.gz"))]
        for input_file in input_files:
            load(kwargs["host"], kwargs["database"], kwargs["username"], kwargs["prefix"], input_file)


def process_user_friend(user_id, collected_at, friend_ids):
    for friend_id in friend_ids:
        yield OrderedDict({
            "collected_at": collected_at,
            "user_id": user_id,
            "friend_id": friend_id,
        })


def process_user_follower(user_id, collected_at, follower_ids):
    for follower_id in follower_ids:
        yield OrderedDict({
            "collected_at": collected_at,
            "user_id": user_id,
            "follower_id": follower_id,
        })


def clean_line(data):
    line = StringIO()
    writer = csv.writer(line, quoting=csv.QUOTE_MINIMAL)
    writer.writerow(data)

    return NULL_TERMINATOR.sub(r"", line.getvalue())


def load(host: str, database: str, username: str, prefix: str, input_file: str):
    print("loading {} into {} on {} on {}".format(input_file, prefix, database, host))
    conn = psycopg2.connect(host=host, dbname=database, user=username)

    try:
        with open(input_file, "rt") as f:
            data = json.load(f)
            collected_at = data["captured_at"]

            if "screen_name" not in data:
                print("could not find screen name information in {}".format(input_file))
                return

            user_obj = {}
            if "user" in data:
                user_obj = data["user"]
                if "favourites_count" in user_obj:
                    user_obj["favorites_count"] = user_obj.pop("favourites_count")
                if "lang" in user_obj:
                    user_obj["language"] = user_obj.pop("lang")
            else:
                user_obj = {
                    "id": data["id"],
                    "id_str": data["id"],
                    "screen_name": data["screen_name"],
                    "created_at": data["created_at"],
                    "name": None,
                    "description": None,
                    "language": None,
                    "location": None,
                    "verified": None,
                    "favorites_count": data["favorites_count"],
                    "statuses_count": data["tweet_count"],
                    "followers_count": None,
                    "friends_count": None,
                }

            follower_ids = None
            if "follower_ids" in data:
                follower_ids = data["follower_ids"]

            friend_ids = None
            if "friend_ids" in data:
                friend_ids = data["friend_ids"]

            with conn.cursor() as cur:
                cur.execute(f"""
                    INSERT INTO public.{prefix}_user (collected_at, id, screen_name, created_at, raw,
                        followers_collected, friends_collected,
                        name, description, location, verified,
                        favorites_count, statuses_count, followers_count, friends_count)
                    VALUES (%(collected_at)s, %(id_str)s, %(screen_name)s, %(created_at)s, %(raw)s,
                        %(followers_collected)s, %(friends_collected)s,
                        %(name)s, %(description)s, %(location)s, %(verified)s,
                        %(favorites_count)s, %(statuses_count)s, %(followers_count)s, %(friends_count)s)
                    ON CONFLICT (collected_at, id) DO UPDATE SET
                        screen_name = excluded.screen_name,
                        created_at = excluded.created_at,
                        raw = excluded.raw,
                        name = excluded.name,
                        description = excluded.description,
                        location = excluded.location,
                        verified = excluded.verified,
                        followers_collected = excluded.followers_collected,
                        friends_collected = excluded.friends_collected,
                        favorites_count = excluded.favorites_count,
                        statuses_count = excluded.statuses_count,
                        followers_count = excluded.followers_count,
                        friends_count = excluded.friends_count
                """, {
                    **user_obj,
                    "collected_at": collected_at,
                    "raw": json.dumps(user_obj),
                    "followers_collected": len(follower_ids) if follower_ids is not None else None,
                    "friends_collected": len(friend_ids) if friend_ids is not None else None,
                })

            if friend_ids:
                generator = (
                    clean_line(x.values())
                    for x in process_user_friend(data["id"], collected_at, friend_ids)
                )
                stream = StringIteratorIO(generator)

                with conn.cursor() as cur:
                    cur.execute(f"CREATE TEMPORARY TABLE t (LIKE public.{prefix}_user_friend) ON COMMIT DROP")
                    cur.copy_expert(f"COPY t FROM STDIN WITH CSV", stream)
                    cur.execute(f"""
                        INSERT INTO public.{prefix}_user_friend
                        SELECT * FROM t
                        ON CONFLICT DO NOTHING
                    """)
                    cur.execute("DROP TABLE t")

            if follower_ids:
                generator = (
                    clean_line(x.values())
                    for x in process_user_follower(data["id"], collected_at, follower_ids)
                )
                stream = StringIteratorIO(generator)

                with conn.cursor() as cur:
                    cur.execute(f"CREATE TEMPORARY TABLE t (LIKE public.{prefix}_user_follower) ON COMMIT DROP")
                    cur.copy_expert(f"COPY t FROM STDIN WITH CSV", stream)
                    cur.execute(f"""
                        INSERT INTO public.{prefix}_user_follower
                        SELECT * FROM t
                        ON CONFLICT DO NOTHING
                    """)
                    cur.execute("DROP TABLE t")

            for tweet in data.get("tweets", []):
                user = tweet["user"]
                retweet = tweet.get("retweeted_status", {})
                quote = tweet.get("quoted_status", {})

                hashtags = set()
                urls = set()

                if "entities" in tweet:
                    hashtags |= set(x["text"] for x in tweet["entities"]["hashtags"])
                    urls |= set(x["expanded_url"] for x in tweet["entities"]["urls"])

                if "extended_tweet" in tweet:
                    hashtags |= set(x["text"] for x in tweet["extended_tweet"]["entities"]["hashtags"])
                    urls |= set(x["expanded_url"] for x in tweet["extended_tweet"]["entities"]["urls"])

                if "entities" in retweet:
                    hashtags |= set(x["text"] for x in retweet["entities"]["hashtags"])
                    urls |= set(x["expanded_url"] for x in retweet["entities"]["urls"])

                if "extended_tweet" in retweet:
                    hashtags |= set(x["text"] for x in retweet["extended_tweet"]["entities"]["hashtags"])
                    urls |= set(x["expanded_url"] for x in retweet["extended_tweet"]["entities"]["urls"])

                if "entities" in quote:
                    hashtags |= set(x["text"] for x in quote["entities"]["hashtags"])
                    urls |= set(x["expanded_url"] for x in quote["entities"]["urls"])

                if "extended_tweet" in quote:
                    hashtags |= set(x["text"] for x in quote["extended_tweet"]["entities"]["hashtags"])
                    urls |= set(x["expanded_url"] for x in quote["extended_tweet"]["entities"]["urls"])

                # remove stupid urls
                urls = set(filter(lambda x: not x.startswith("https://twitter.com/i/web/status/"), urls))

                with conn.cursor() as cur:
                    cur.execute(f"""
                        INSERT INTO public.{prefix}_tweet (
                            id, created_at, tweet, source, language, user_id, user_screen_name,
                            in_reply_to_status_id, in_reply_to_user_id, in_reply_to_user_screen_name,
                            retweeted_status_id, retweeted_status_user_id, retweeted_status_user_screen_name, retweeted_status_user_name, retweeted_status_user_description,
                            retweeted_status_user_friends_count, retweeted_status_user_statuses_count, retweeted_status_user_followers_count,
                            retweeted_status_retweet_count, retweeted_status_favorite_count, retweeted_status_reply_count,
                            quoted_status_id, quoted_status_user_id, quoted_status_user_screen_name, quoted_status_user_name, quoted_status_user_description,
                            quoted_status_user_friends_count, quoted_status_user_statuses_count, quoted_status_user_followers_count,
                            quoted_status_retweet_count, quoted_status_favorite_count, quoted_status_reply_count,
                            hashtags, urls, raw
                        ) VALUES (
                            -- id (7)
                            %s, %s, %s, %s, %s, %s, %s,
                            -- in_reply_to_status_id (3)
                            %s, %s, %s,
                            -- retweeted_status_id (5)
                            %s, %s, %s, %s, %s,
                            -- retweeted_status_user_friends_count (3)
                            %s, %s, %s,
                            -- retweeted_status_retweet_count (3)
                            %s, %s, %s,
                            -- quoted_status_id (5)
                            %s, %s, %s, %s, %s,
                            -- quoted_status_user_friends_count
                            %s, %s, %s,
                            -- quoted_status_retweet_count
                            %s, %s, %s,
                            -- hashtags, urls, raw
                            %s, %s, %s
                        ) ON CONFLICT (id) DO NOTHING
                    """, [
                        tweet["id_str"], tweet["created_at"], get_complete_text(tweet), get_source(tweet["source"]), tweet["lang"], user["id_str"], user["screen_name"],
                        tweet["in_reply_to_status_id_str"], tweet["in_reply_to_user_id_str"], tweet["in_reply_to_screen_name"],
                        retweet.get("id_str"), retweet.get("user", {}).get("id_str"), retweet.get("user", {}).get("screen_name"), retweet.get("user", {}).get("user_name"), retweet.get("user", {}).get("description"),
                        retweet.get("user", {}).get("friends_count"), retweet.get("user", {}).get("statuses_count"), retweet.get("user", {}).get("followers_count"),
                        retweet.get("retweet_count"), retweet.get("favorite_count"), retweet.get("reply_count"),
                        quote.get("id_str"), quote.get("user", {}).get("id_str"), quote.get("user", {}).get("screen_name"), quote.get("user", {}).get("user_name"), quote.get("user", {}).get("description"),
                        quote.get("user", {}).get("friends_count"), quote.get("user", {}).get("statuses_count"), quote.get("user", {}).get("followers_count"),
                        quote.get("retweet_count"), quote.get("favorite_count"), quote.get("reply_count"),
                        list(hashtags), list(urls), json.dumps(tweet),
                    ])

        conn.commit()
    except Exception:
        traceback.print_exc()
        try:
            self.cnx.rollback()
        except Exception:
            pass


if __name__ == "__main__":
    username = getpass.getuser()

    parser = argparse.ArgumentParser(
        prog="load_networks",
        formatter_class=argparse.RawTextHelpFormatter,
        description=__doc__,
    )
    parser.add_argument("input", nargs="?", help="a directory full of files with tweets to load")
    parser.add_argument("--host", help="the database cluster to load the data into", default="venus.lab.cip.uw.edu")
    parser.add_argument("-d", "--database", help="the database to load the data into", default=username)
    parser.add_argument("-u", "--username", help="the name of the user to use when connecting to the database", default=username)
    parser.add_argument("-p", "--prefix", help="the prefix of the database tables to load this into", required=True)
    args = parser.parse_args()

    try:
        main(**vars(args))
    except Exception:
        traceback.print_exc()
