# twitter apiv1 tools

This includes a few different scripts. These aren't perfect and you may need to
tweak them to meet your needs.

## Getting Started

Before you can use any of them you need to create a file with your Twitter
APIv1 credentials. It should look like this:

```jsonfile
[
    {
        "consumer_key": "ksldafjlaksdj",
        "consumer_secret": "asdkfjakl",
        "access_token": "asdfkljaklsdjf",
        "access_token_secret": "alskdf"
    }
]
```

Put your credentials in there and then lock the file by running this command,
changing the name of the file to match the name of the file that you just
created:

```
chmod 600 credentials.json
```

You can include multiple credentials in the file if you have multiple
credentials to use. This will make your searches faster.

## Libraries

These tools require libraries that are not installed by default. You will need
to create a virtual environment to use them. To do that, run these steps:

```
python3 -m venv venv
source venv/bin/activate
pip3 install -r requirements.txt
```

This will install the necessary requirements to use these tools. It will also
make all of the other libraries unavailable to you. If you need more libraries
available to you then you can simply install them into your virtual environment
in the same way:

```
pip3 install mylib
```

## Output

These scripts output their results to `stdout` and their logs to `stderr`. So
when you run then you should run them like this:

```
python3 get_statistics.py credentials.json account_ids.csv > output.csv
```

The `>` operator redirects `stdout` to the file but leaves `stderr` writing to
the console so that you can still see it.

## Caveats

This will not return tweets that have been deleted or where the account has
been deleted or suspended. All values are accurate at the time of the search,
according to Twitter.

## get_account_ids.py

When given a file with account names, one per line, this will return account
IDs, one per line. If the account does not exist or has been suspended then it
will output that, too.

For example:

```
python3 get_account_ids.py credentials.json account_names.csv > account_ids.csv
```

## get_statistics.py

When given a file with IDs, one per line, this will return statistics about
those accounts such as how many friends or followers the account has. If the
account does not exist or has been suspended then it will output that, too.

For example:

```
python3 get_statistics.py credentials.json account_ids.csv > account_stats.csv
```

Included in the statistics are:

* `user_id` - The Twitter ID for this account.
* `user_screen_name` - The current screen name for this account.
* `captured_at` - The time at which these statistics were captured.
* `created_at` - When the account was created.
* `followers` - How many followers this account currently has.
* `friends` - How many friends this account currently has.
* `favorites` - How many tweets this account has favorited.
* `statuses` - How many tweets this account has posted.

## get_networks.py

When given a file with IDs, one per line, this will return varying amounts of
information depending on the flags. By default it will write one JSON file per
account and include in it basic statistics about the account such as when it
was created and how many friends and followers it has. You can add additional
flags to get more information.

* `--fetch-tweets` - Fetch the most recent 2000 or so tweets by this account.
* `--fetch-friends` - Fetch all friends of this account.
* `--fetch-followers` - Fetch all followers of this account.

Be warned that fetching followers can take a very long time. For example, if
you try to get all of the followers of BarackObama it will take several days.

The arguments to this program are:

* `credentials` - A file containing Twitter APIv1 credentials.
* `accounts` - A file containing one Twitter User ID per line.
* `output` - A path where output files will be written.

One output file is written per account ID in the output directory. If a file
already exists for the given user ID then a new file will not be written. Be
aware that if you "cancel" the script (using Ctrl-C or something) you might not
get a file with all of the data that you want if it was in the middle of
processing that account and had written a partial file.

It is highly recommended that you run this command in a `screen` session. See
[the wiki](https://www.lab.cip.uw.edu/wiki/Frequently_Asked_Questions#Starting_Long_Running_Programs) for more details.
