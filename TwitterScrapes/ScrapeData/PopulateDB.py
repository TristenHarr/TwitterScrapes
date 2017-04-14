from TwitterScrapes.ScrapeData import tagger
from TwitterScrapes.ScrapeData.InRoute.DeepStore import store_it

def go(search_for=list(), search_type='track',
       limit=60, limit_type='time', table_name='',
       database_name='', lang=None, location_geo_box_lookup=None):
    """
    Sets the twitter search fields and turns the scraper on
    :type search_for: list
    :param search_for: A list of the terms to search for
    :type search_type: str
    :param search_type: Either 'track' or 'location'
    :type limit: int
    :param limit: The limit to scrape to
    :type limit_type: str
    :param limit_type: Either 'count' or 'time'
    :type table_name: str
    :param table_name: The table to insert the data into
    :type database_name: str
    :param database_name: The database to insert the data into
    :type lang: list
    :param lang: A list of languages to scrape for
    :type location_geo_box_lookup: str
    :param location_geo_box_lookup: A 2 letter uppercase country code to search for tweets in
    :return: None
    """
    item = open("../ScrapeData/InRoute/limit.txt", 'w')
    item.write("{} {}".format(limit, limit_type.upper()))
    item.close()
    track = tagger.track
    locate = tagger.location
    lookup = tagger.loc_lookup
    if search_type == 'track':
        if lang is not None:
            track(search_for, languages=lang)
        else:
            track(search_for)
    elif search_type == 'location':
        if lang is not None:
            if location_geo_box_lookup is not None:
                locate(lookup(location_geo_box_lookup), languages=lang)
            else:
                locate(search_for, languages=lang)
        else:
            if location_geo_box_lookup is not None:
                locate(lookup(location_geo_box_lookup))
            else:
                locate(search_for)
    store_it(table_name, database_name)
