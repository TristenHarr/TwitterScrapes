from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import time
import sqlite3
import random
import re
from serverside.get_keys import retrieval


class Extraction(object):

    def __init__(self):
        self.data = None
        self.conn = None
        self.database = None
        self.table = None
        self.manage = None

    def connect(self, database, table_name):
        self.table = table_name
        self.database = database
        # Connects to a SQLite database with the specified database name
        self.conn = sqlite3.connect("DATABASE/" + database + ".sqlite")
        # Creates a table with the specified table name if it doesn't already exist
        self.conn.execute("CREATE TABLE IF NOT EXISTS {tn} (favorites TEXT,"
                          "followers INTEGER,"
                          "hashtags TEXT,"
                          "in_reply_to TEXT,"
                          "lang TEXT,"
                          "links_mentioned TEXT,"
                          "original_author_handle TEXT,"
                          "original_author_id TEXT,"
                          "place TEXT,"
                          "plain_text TEXT,"
                          "source TEXT,"
                          "user_statuses TEXT,"
                          "tweet TEXT,"
                          "tweet_id TEXT,"
                          "tweet_location TEXT,"
                          "tweet_mentions TEXT,"
                          "tweeted_time TEXT,"
                          "user_twitter_birthday TEXT,"
                          "user_description TEXT,"
                          "user_handle TEXT,"
                          "user_id TEXT,"
                          "user_location TEXT,"
                          "user_name TEXT, PRIMARY KEY (tweet_id))".format(tn=table_name))
        self.manage = DataManager()
        self.manage.connect()
        self.manage.addition(database, table_name)

    def locate(self):
        if self.data['coordinates'] is not None:
            my_location = self.data['coordinates']['coordinates']
            return {'tweet_location': '`'.join(list(map(lambda x: str(x), my_location)))}
        # If they don't exist, but place does exist, look for a coordinates bounding box
        elif self.data['place'] is not None and self.data['place']['bounding_box']['coordinates']:
            boxes = self.data['place']['bounding_box']['coordinates'][0]
            my_lat = [boxes[0][1], boxes[1][1]]
            my_long = [boxes[0][0], boxes[2][0]]
            my_lat_range = random.randint(int(my_lat[0] * 100000), int(my_lat[1] * 100000)) / 100000
            my_long_range = random.randint(int(my_long[0] * 100000), int(my_long[1] * 100000)) / 100000
            return {'tweet_location': str(my_long_range) + '`' + str(my_lat_range)}
        # If the above fails, just return None
        else:
            return {'tweet_location': 'None'}

    def user_data(self):
        itemuser = self.data['user']
        my_user_dict = {'user_id': itemuser['id'], 'user_name': itemuser['name'],
                        'user_handle': itemuser['screen_name'], 'user_desc': itemuser['description'],
                        'twitter_birthday': itemuser['created_at'], 'user_location': itemuser['location'],
                        'followers': itemuser['followers_count'], 'favorites': itemuser['favourites_count'],
                        'statuses': itemuser['statuses_count']}
        return my_user_dict

    def entities_data(self):
        entities_item = self.data['entities']
        my_entities_dict = {"hashtags": ""}
        for tag in entities_item['hashtags']:
            # Delimits hashtags with ` this is used mainly for simplicity reasons, not every tweet
            # has a hashtag, so making another table column with the hashtags didn't make sense
            my_entities_dict['hashtags'] += tag['text'] + '`'
        my_entities_dict['tweet_mentions'] = ""
        my_entities_dict['links_mention'] = ''
        for person in entities_item['user_mentions']:
            # This is similar to the above
            my_entities_dict['tweet_mentions'] += person['id_str'] + '`'
        for links in entities_item['urls']:
            # Similar to the above
            my_entities_dict['links_mention'] += links['url'] + '`'
        return my_entities_dict

    def extract_relevant(self):
        item_extraction = self.data
        my_dict = {'tweeted_time': item_extraction['created_at'],
                   'tweet_id': item_extraction['id'],
                   'in_reply_to':
                       "NAME/" + str(item_extraction['in_reply_to_screen_name']) + "`" +
                       "STATUSID/" + str(item_extraction['in_reply_to_status_id_str']) + "`" +
                       "USERID/" + str(item_extraction['in_reply_to_user_id_str']),
                   'lang': item_extraction['lang'],
                   'place': item_extraction['place'], 'source': item_extraction['source']}
        if item_extraction['place'] is not None:
            my_dict['place'] = item_extraction['place']['full_name']
        # The goal is to make the database as clean and easy to use as possible, this makes it
        # much easier to check if a tweet was in reply to another tweet, without having to look up 3 values
        if 'retweeted_status' in item_extraction.keys():
            my_dict['original_author_id'] = item_extraction['retweeted_status']['user']['id']
            my_dict['original_author_handle'] = item_extraction['retweeted_status']['user']['screen_name']
            tester = item_extraction['retweeted_status']['text']
            # Python regex seemed like the quickest way to take care of the below.
            cleaned = ' '.join(re.sub("(RT : )|(@[\S]+)|(&\S+)|(http\S+)", " ", tester).split())
            removed_others = " ".join(re.sub("(#\S+)", ' ', cleaned).split())
            final_text = ''.join(list(filter(lambda x: x.isalpha() or x is ' ', removed_others)))
            # This final text will make it a lot easier to run NLP
            final_text = final_text.strip().replace('   ', ' ').replace('  ', ' ')
            my_dict['plain_text'] = final_text
            my_dict['tweet'] = cleaned
        else:
            my_dict['original_author_id'] = item_extraction['user']['id']
            my_dict['original_author_handle'] = item_extraction['user']['screen_name']
            cleaned = ' '.join(re.sub("(@[\S]+)|(&\S+)|(http\S+)", " ", item_extraction['text']).split())
            removed_others = " ".join(re.sub("(#\S+)", ' ', cleaned).split())
            final_text = ''.join(list(filter(lambda x: x.isalpha() or x is ' ', removed_others)))
            final_text = final_text.strip().replace('   ', ' ').replace('  ', ' ')
            my_dict['plain_text'] = final_text
            my_dict['tweet'] = cleaned
        return my_dict

    def store_data(self, data):
        self.data = data
        the_main_dict = {**self.user_data(), **self.entities_data(), **self.extract_relevant(), **self.locate()}
        my_keys_list = sorted(the_main_dict.keys())
        my_items = list(map(lambda x: str(the_main_dict[x]).replace("'", ''), my_keys_list))
        try:
            # Unpacks the items into an insert statement for the SQLite table
            self.conn.execute("INSERT INTO {0} VALUES('{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}',"
                              "'{10}','{11}','{12}','{13}','{14}','{15}','{16}','{17}','{18}','{19}','{20}',"
                              "'{21}','{22}','{23}')".format(self.table, *my_items))
        except sqlite3.IntegrityError:
            pass

    def finish(self):
        self.conn.commit()
        self.conn.close()


class DataManager(object):

    def __init__(self):
        self.conn = None

    def connect(self):
        self.conn = sqlite3.connect("DATABASE/CONTROL.sqlite")
        self.conn.execute("CREATE TABLE IF NOT EXISTS {tn} (id TEXT, database TEXT,"
                          "query TEXT, query_text TEXT,"
                          " volume INTEGER, PRIMARY KEY (id, database))".format(tn='manager'))

    def addition(self, database, table):
        try:
            self.conn.execute("INSERT INTO manager VALUES('{id}','{database}','{query}', '{query_text}'"
                              ",'{volume}')".format(id=table, database=database, query="None", query_text="None",
                                                    volume="None"))
            self.conn.commit()
            self.conn.close()
            return True
        except sqlite3.IntegrityError:
            return False


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
        self.database = None
        super(MyListener, self).__init__()

    def config(self, database, table_name):
        self.database = Extraction()
        self.database.connect(database, table_name)

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
                item = json.loads(data)
                if 'created_at' in item.keys():
                    self.temp |= set(item.keys())
                    self.database.store_data(item)
                else:
                    pass
                # The below is a check to prevent entering another loop by forcing the stream to be cutoff
                if (time.time() - self.start_time) < self.limit:
                    return True
                else:
                    self.database.finish()
                    return False
            # If the limit type is count, and the number of tweets streamed in hasn't reached the limit, continue
            elif self.limit_type == "COUNT" and self.limit != 0:
                # See limit type = 'TIME' for confusion about below
                item = json.loads(data)
                if 'created_at' in item.keys():
                    self.database.store_data(item)
                    self.limit -= 1
                else:
                    pass
                if self.limit > 0:
                    return True
                else:
                    self.database.finish()
                    # Call counter
                    return False
            else:
                return False
        except BaseException as e:
            print(e)
            return True

    def on_status(self, status):
        print(status.txt)

    def on_limit(self, limit_info):
        print(limit_info)
        return


def locate(code):
    """
    Used to get the geo-box coordinates of a country by it's code EX: 'US'

    :type code: str
    :param code: A :str: with a country code
    :return: A :list: of :float: with geo-box coordinates
    """
    my_dict = {}
    # Gets the specified country codes geo-box coordinates from a CVS file
    with open("CSV-Files/country-boundingboxes.csv", 'r') as tester:
        for line in tester:
            item = line.rstrip('\n').split(',')
            my_dict[item[0]] = list(map(lambda x: float(x), item[2:]))
    tester.close()
    return my_dict[code]


class Scraper(object):

    def __init__(self, search_type):
        self.search = search_type
        self.items = None
        self.lang = None
        self.limit = None
        self.limit_type = None
        self.database = None
        self.table = None

    def set_limit(self, limit_type, limit):
        self.limit_type = limit_type
        self.limit = limit

    def databse_config(self, database, table):
        self.table = table
        self.database = database

    def search_configure(self, search=list()):
        self.items = search

    def set_languages(self, languages):
        self.lang = languages

    def scrape(self, password):
        secrets = retrieval(password)
        access_token = secrets[0]
        access_token_secret = secrets[1]
        consumer_key = secrets[2]
        consumer_secret = secrets[3]
        scraper = MyListener(self.limit, self.limit_type)
        scraper.config(self.database, self.table)
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        stream = Stream(auth, scraper)
        if self.search == 'track' and self.lang is None:
            stream.filter(track=self.items)
        elif self.search == 'track':
            stream.filter(track=self.items, languages=self.lang)
        elif self.search == 'location' and self.lang is None:
            stream.filter(locations=self.items)
        elif self.search == 'location':
            stream.filter(locations=self.items, languages=self.lang)

thing = Scraper('track')
thing.set_limit('COUNT', 100)
thing.set_languages(['en'])
thing.search_configure(["dare"])
thing.databse_config("Bored", "stuff")
thing.scrape('tristen')