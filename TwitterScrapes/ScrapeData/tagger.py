# ScrapeData.py
"""Streams Data in using the Twitter API. Built on top of tweepy
Author: Tristen Harr
Date:   4/7/2017"""

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import time





class MyListener(StreamListener):

    def __init__(self, limits=60, limit_types="TIME"):
        """
        Extends StreamListener in the tweepy module

        :type limits: int
        :param limits: The limit of the amount of data to stream
        :type limit_types: str
        :param limit_types: Specify 'TIME' for seconds or 'COUNT' for tweet number
        """
        self.start_time = time.time()
        self.limit_type = limit_types
        self.limit = limits
        self.temp = set()
        super(MyListener, self).__init__()

    def on_data(self, data):
        """
        Extends the
        :param data: Data streamed from twitter API through tweepy
        :return: True to continue stream, False to end stream
        """
        # Try-Except used by tweepy
        try:
            # If the limit type is time, and the time passed is less than the limit, continue
            if self.limit_type == "TIME" and (time.time() - self.start_time < self.limit):
                # Opens a .txt file to determine which pool to send collected tweets to
                work_order = open('../ScrapeData/InRoute/work_order.txt', 'r')
                work_orders = work_order.readline()
                work_order.close()
                # Opens the pooling file, this was done purposefully. This prevents errors with database
                # interaction, allowing tweets to be streamed in as quickly as possible and sent directly
                # into a pool to be cleared upon conclusion of the streaming period. This also allows easier
                # cleaning of the data before it is placed into the database
                # By closing the file with each tweet, a disconnect will be much less likely to corrupt data
                my_file = open('../ScrapeData/InRoute/work{}.txt'.format(work_orders), 'a', encoding="utf-8")
                item = json.loads(data)
                if 'created_at' in item.keys():
                    self.temp |= set(item.keys())
                    my_file.write(json.dumps(item)+'\n')
                    my_file.close()
                else:
                    my_file.close()
                # The below is a check to prevent entering another loop by forcing the stream to be cutoff
                if (time.time() - self.start_time) < self.limit:
                    return True
                else:
                    return False
            # If the limit type is count, and the number of tweets streamed in hasn't reached the limit, continue
            elif self.limit_type == "COUNT" and self.limit != 0:
                # See limit type = 'TIME' for confusion about below
                work_order = open('../ScrapeData/InRoute/work_order.txt', 'r')
                work_orders = work_order.readline()
                work_order.close()
                my_file = open('../ScrapeData/InRoute/work{}.txt'.format(work_orders), 'a', encoding="utf-8")
                self.limit -= 1
                item = json.loads(data)
                if 'created_at' in item.keys():
                    my_file.write(json.dumps(item)+"\n")
                    my_file.close()
                else:
                    my_file.close()
                if self.limit > 0:
                    return True
                else:
                    return False
            else:
                return False
        # TODO Is BaseException required? Build custom exceptions
        except BaseException as e:
            print(e)
            return True

    def on_status(self, status):
        print(status.txt)

    def on_limit(self, limit_info):
        print(limit_info)
        return


def track(track_list, languages=False):
    """
    Pulls in a stream from the twitter API with the specified keywords using tweepy

    :type track_list: list
    :param track_list: A list of :str: containing the keywords to search for
    :type languages: list
    :param languages: A list of :str: containing languages to include in the stream
    :return:
    """
    if languages:
        stream.filter(track=track_list, languages=languages)
    else:
        stream.filter(track=track_list)


def location(location_box, languages=False):
    """
    Pulls in a stream from the twitter API within a specified geo-location-box

    :type location_box: list
    :param location_box: A list of :float: coordinates that represent a geo-box
    :type languages: list
    :param languages: A list of :str: containing the languages to be included in the stream
    :return:
    """
    if languages:
        stream.filter(locations=location_box, languages=languages)
    else:
        stream.filter(locations=location_box)


def loc_lookup(code):
    """
    Used to get the geo-box coordinates of a country by it's code EX: 'US'

    :type code: str
    :param code: A :str: with a country code
    :return: A :list: of :float: with geo-box coordinates
    """
    my_dict = {}
    # Gets the specified country codes geo-box coordinates from a CVS file
    with open("../ScrapeData/CSV-Files/country-boundingboxes.csv", 'r') as tester:
        for line in tester:
            item = line.rstrip('\n').split(',')
            my_dict[item[0]] = list(map(lambda x: float(x), item[2:]))
    tester.close()
    return my_dict[code]

if "__main__":
    # The below is used to specify the stream limits it was the simplest and
    # easiest fix to prevent threading errors, and also to allow for changes to the above
    # code to more easily be made
    secrets = open("../ScrapeData/InRoute/lockdown.txt", 'r')
    access_token = next(secrets).strip()
    access_token_secret = next(secrets).strip()
    consumer_key = next(secrets).strip()
    consumer_secret = next(secrets).strip()
    secrets.close()
    lims = open("../ScrapeData/InRoute/limit.txt", 'r')
    the_lims = lims.readline().split(' ')
    lims.close()
    limit = int(the_lims[0])
    limit_type = the_lims[1]
    l = MyListener(limit, limit_type)
    # The below handles the OAuth *Thanks tweepy!!
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
