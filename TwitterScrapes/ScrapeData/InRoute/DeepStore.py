# DeepStore.py
"""Stores data pulled in from a file written by #Tagger.py into a SQLite database
Author: Tristen Harr
Date: 4/7/2017
"""
# TODO make it possible for users to make custom store_it parameters
import json
import random
import sqlite3
import re


def store_it(table_name, database):
    """
    :type table_name: str
    :param table_name: The name of the table to store the information in
    :type database: str
    :param database: The name of the database to store the information in
    :return:
    """
    # Determines which pool the information is stored in
    changer = open("../ScrapeData/InRoute/work_order.txt", 'r')
    switch = changer.readline()
    changer.close()
    # Re-routes any incoming tweets into the back-up pool to prevent collisions
    changer = open('../ScrapeData/InRoute/work_order.txt', 'w')
    if switch == "1":
        changer.write('2')
    else:
        changer.write('1')
    changer.close()
    table = [table_name, database]
    # Connects to a SQLite database with the specified database name
    conn = sqlite3.connect("../ScrapeData/DATABASE/"+table[1] + ".sqlite")
    # Creates a table with the specified table name if it doesn't already exist
    conn.execute("CREATE TABLE IF NOT EXISTS {tn} (favorites TEXT,"
                 "followers TEXT,"
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
                 "user_name TEXT, PRIMARY KEY (tweet_id))".format(tn=table[0]))

    def locate(itemlocate):
        # TODO Allow the size for random location boxes to be specified at a State, County, City level
        """
        Attempts to determine a geo-graphical location of the tweet/user, if a location is determined
        but not exact, a geo-coordinate location is randomly generated within the constraints of the geo-box
        to allow for choropleth plotting

        :type itemlocate: dict
        :param itemlocate: A dictionary loaded in from the twitter API with tweepy
        :return: A dict of the geo-coordinates
        """
        # If the coordinates exist, return the coordinates
        if itemlocate['coordinates'] is not None:
            my_location = itemlocate['coordinates']['coordinates']
            return {'tweet_location': '`'.join(list(map(lambda x: str(x), my_location)))}
        # If they don't exist, but place does exist, look for a coordinates bounding box
        elif itemlocate['place'] is not None and itemlocate['place']['bounding_box']['coordinates']:
            boxes = itemlocate['place']['bounding_box']['coordinates'][0]
            my_lat = [boxes[0][1], boxes[1][1]]
            my_long = [boxes[0][0], boxes[2][0]]
            my_lat_range = random.randint(int(my_lat[0] * 100000), int(my_lat[1] * 100000)) / 100000
            my_long_range = random.randint(int(my_long[0] * 100000), int(my_long[1] * 100000)) / 100000
            return {'tweet_location': str(my_long_range)+'`'+str(my_lat_range)}
        # If the above fails, just return None
        else:
            return {'tweet_location': 'None'}

    def user_data(itemuser):
        """
        Creates a cleaned up dictionary of the user portion

        :type itemuser: dict
        :param itemuser: A dictionary loaded in from the twitter API with tweepy
        :return: A dictionary with the useful information
        """
        itemuser = itemuser['user']
        my_user_dict = {'user_id': itemuser['id'], 'user_name': itemuser['name'],
                        'user_handle': itemuser['screen_name'], 'user_desc': itemuser['description'],
                        'twitter_birthday': itemuser['created_at'], 'user_location': itemuser['location'],
                        'followers': itemuser['followers_count'], 'favorites': itemuser['favourites_count'],
                        'statuses': itemuser['statuses_count']}
        return my_user_dict

    def entities_data(entities_item):
        """
        Creates a dictionary of useful things from the entities data

        :type entities_item: dict
        :param entities_item: A dictionary loaded in from the twitter API with tweepy
        :return: A dictionary with the useful information
        """
        entities_item = entities_item['entities']
        my_entities_dict = {"hashtags": ""}
        for tag in entities_item['hashtags']:
            # Delimits hashtags with ` this is used mainly for simplicity reasons, not every tweet
            # has a hashtag, so making another table column with the hashtags didn't make sense
            my_entities_dict['hashtags'] += tag['text'] + '`'
        my_entities_dict['tweet_mentions'] = ""
        my_entities_dict['links_mention'] = ''
        for person in entities_item['user_mentions']:
            # This is similar to the above
            my_entities_dict['tweet_mentions'] += person['id_str']+'`'
        for links in entities_item['urls']:
            # Similar to the above
            my_entities_dict['links_mention'] += links['url'] + '`'
        return my_entities_dict

    def extract_relevant(item_extraction):
        """
        Extracts relevant information from the stream to store into the database

        :type item_extraction: dict
        :param item_extraction: A dictionary loaded in from the twitter API with tweepy
        :return: A dictionary with useful information
        """
        my_dict = {'tweeted_time': item_extraction['created_at'],
                   'tweet_id': item_extraction['id'],
                   'in_reply_to':
                       "NAME/"+str(item_extraction['in_reply_to_screen_name']) + "`" +
                       "STATUSID/"+str(item_extraction['in_reply_to_status_id_str']) + "`" +
                       "USERID/"+str(item_extraction['in_reply_to_user_id_str']),
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

    # Opens the file to load in the tweets
    data = open("../ScrapeData/InRoute/work{}.txt".format(switch), 'r', encoding="utf-8")
    for line in data:
        item = json.loads(line)
        # The below takes advantage of dictionary unpacking, the return type of the 3 functions used
        # are dictionaries, so those three dictionaries are being unpacked into a larger dictionary
        the_main_dict = {**user_data(item), **entities_data(item), **extract_relevant(item), **locate(item)}
        # Sorts the keys to make insertion into the database go seamlessly, joins could have been used
        # but it made more sense to just sort them ahead of time
        # TODO the below line can be specified outside of the loop explicitly, and will offer a decent speedup
        my_keys_list = sorted(the_main_dict.keys())
        # Makes a sorted list of the values contained
        my_items = list(map(lambda x: str(the_main_dict[x]).replace("'", ''), my_keys_list))
        try:
            # Unpacks the items into an insert statement for the SQLite table
            conn.execute("INSERT INTO {0} VALUES('{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}',"
                         "'{12}','{13}','{14}','{15}','{16}','{17}','{18}','{19}','{20}','{21}','{22}',"
                         "'{23}')".format(table[0],*my_items))
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    conn.close()
    data.close()
    # Clears out the pool since all the data has been loaded into tha database
    clean = open("../ScrapeData/InRoute/work{}.txt".format(switch), 'w', encoding="utf-8")
    clean.write('')
    clean.close()
