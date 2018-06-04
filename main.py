from tweetsearcher import TweetSearcher, random_proxy
import numpy as np
import pandas as pd
import ray
import random

ray.init()

proxy_list = [
    '212.19.134.214:34040',
    '188.120.241.73:58637',
    '188.120.247.44:27840',
    '80.211.29.123:7708',
    '172.246.69.216:58670',
    # '46.162.124.254:21084',
    # '210.186.156.119:8181',
    # '178.212.176.82:1080',
    # '5.101.64.68:55012'
]


@ray.remote
def download_tweets(params):
    proxy = random.choice(proxy_list)
    print('Start task on proxy %s' % proxy)
    searcher = TweetSearcher(proxy='socks5h://%s' % proxy, debug=True)
    searcher.tweets_to_csv(**params)


class Range:
    def __init__(self, since, until):
        self.since = since
        self.until = until


if __name__ == '__main__':
    # start_date = '2013-06-16'
    # end_date = '2013-06-26'
    q = 'bitcoin'

    # date_range = pd.period_range(start_date, end_date)
    # split_count = int(len(date_range) / 10)
    # split_date_ranges = np.array_split(date_range.values, split_count)
    date_ranges = [
        Range('2013-01-01', '2013-01-10'),
        Range('2013-01-10', '2013-01-20'),
        Range('2013-01-20', '2013-02-01'),

        Range('2013-02-01', '2013-02-10'),
        Range('2013-02-10', '2013-02-20'),
        Range('2013-02-20', '2013-03-01'),

        Range('2013-03-01', '2013-03-10'),
        Range('2013-03-10', '2013-03-20'),
        Range('2013-03-20', '2013-04-01'),

        Range('2013-04-01', '2013-04-10'),
        Range('2013-04-10', '2013-04-20'),
        Range('2013-04-20', '2013-05-01'),

        Range('2013-05-01', '2013-05-10'),
        Range('2013-05-10', '2013-05-20'),
        Range('2013-05-20', '2013-06-01'),

        Range('2013-06-01', '2013-06-10'),
        Range('2013-06-10', '2013-06-20'),
        Range('2013-06-20', '2013-07-01'),

        Range('2013-07-01', '2013-07-10'),
        Range('2013-07-10', '2013-07-20'),
        Range('2013-07-20', '2013-08-01'),

        Range('2013-08-01', '2013-08-10'),
        Range('2013-08-10', '2013-08-20'),
        Range('2013-08-20', '2013-09-01'),

        Range('2013-09-01', '2013-09-10'),
        Range('2013-09-10', '2013-09-20'),
        Range('2013-09-20', '2013-10-01'),

        Range('2013-10-01', '2013-10-10'),
        Range('2013-10-10', '2013-10-20'),
        Range('2013-10-20', '2013-11-01'),

        Range('2013-11-01', '2013-11-10'),
        Range('2013-11-10', '2013-11-20'),
        Range('2013-11-20', '2013-12-01'),

        Range('2013-12-01', '2013-12-10'),
        Range('2013-12-10', '2013-12-20'),
        Range('2013-12-20', '2014-01-01'),
    ]
    ray.get(
        [download_tweets.remote({'q': q, 'since': range.since, 'until': range.until, 'remove_if_exist': True}) for range
         in date_ranges])
