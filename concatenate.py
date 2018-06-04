import pandas as pd
import numpy as np
import os
import csv

if __name__ == '__main__':
    date_parser = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
    directory = '2012'
    files_names = os.listdir(directory)
    csv_list = []

    for name in files_names:
        csv = pd.read_csv(
            '%s/%s' % (directory, name),
            # index_col=['id'],
            parse_dates=['datetime'],
            date_parser=date_parser,
            encoding='utf-8',
        )
        csv_list.append(csv)

    result = pd.concat(csv_list, sort=False, ignore_index=True)
    # result.sort_index(inplace=True)
    # result = result.drop('Unnamed: 0', 1)
    result.sort_values(by='datetime', ascending=True, inplace=True)
    result.to_csv('%s/tweets_bitcoin.csv' % directory, index=False)

    csv = pd.read_csv('%s/tweets_bitcoin.csv' % directory,
                      # index_col=['id'],
                      parse_dates=['datetime'],
                      date_parser=date_parser,
                      encoding='utf-8')

    print(csv.head())
    print(len(csv))
