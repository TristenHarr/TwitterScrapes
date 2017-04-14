from TwitterScrapes.ScrapeData.PopulateDB import go
from TwitterScrapes.ScrapeData.DATABASE.DBmanager import Table, Statement, DataBaseSearch


class Scraper(object):

    def __init__(self, search_type):
        """
        A Scraper Object, with methods for extracting data from twitter.

        :type search_type: str
        :param search_type: 'location' or 'track'
        """
        self._search_type = search_type
        self._limit_type = None
        self._limit = None
        self._lang = None
        self._database_name = None
        self._database_table = None
        self._location_lookup = None
        self._search_for = []
        self._location_geo_box = []

    def __str__(self):
        if self._search_type.lower() == "location":
            if self._location_lookup is not None:
                if self._lang is not None:
                    if self._limit_type.lower() == "time":
                        answer = "\
Search for tweets in the {} that are in {}\n\
Place these tweets into the database {} in the table {}\n\
Run the scraper for {} seconds\n\
                    ".format(self._location_lookup, self._lang, self._database_name,
                             self._database_table, self._limit)
                        return answer
                    else:
                        answer = "\
Search for tweets in the {} that are in {}\n\
Place these tweets into the database {} in the table {}\n\
Collect {} tweets\n\
                        ".format(self._location_lookup, self._lang, self._database_name,
                                 self._database_table, self._limit)
                        return answer
                else:
                    if self._limit_type.lower() == "time":
                        answer = "\
Search for tweets in the {}\n\
Place these tweets into the database {} in the table {}\n\
Run the scraper for {} seconds\n\
                    ".format(self._location_lookup, self._database_name, self._database_table, self._limit)
                        return answer
                    else:
                        answer = "\
Search for tweets in the {}\n\
Place these tweets into the database {} in the table {}\n\
Collect {} tweets\n\
                        ".format(self._location_lookup, self._database_name, self._database_table, self._limit)
                        return answer
            elif len(self._location_geo_box) > 0:
                if self._lang is not None:
                    if self._limit_type.lower() == "time":
                        answer = "\
Search for tweets in the {} that are in {}\n\
Place these tweets into the database {} in the table {}\n\
Run the scraper for {} seconds\n\
                    ".format(self._search_for, self._lang, self._database_name,
                             self._database_table, self._limit)
                        return answer
                    else:
                        answer = "\
Search for tweets in the {} that are in {}\n\
Place these tweets into the database {} in the table {}\n\
Collect {} tweets\n\
                        ".format(self._location_lookup, self._lang, self._database_name,
                                 self._database_table, self._limit)
                        return answer
                else:
                    if self._limit_type.lower() == "time":
                        answer = "\
Search for tweets in the {}\n\
Place these tweets into the database {} in the table {}\n\
Run the scraper for {} seconds\n\
                    ".format(self._location_lookup, self._database_name, self._database_table, self._limit)
                        return answer
                    else:
                        answer = "\
Search for tweets in the {}\n\
Place these tweets into the database {} in the table {}\n\
Collect {} tweets\n\
                        ".format(self._location_lookup, self._database_name, self._database_table, self._limit)
                        return answer
            else:
                return "The search type is specified as location, but no location has been set"
        elif self._search_type.lower() == 'track':
            if self._lang is not None:
                if self._limit_type.lower() == "time":
                    answer = "\
Search for tweets containing the keywords {} that are in {}\n\
Place these tweets into the database {} in the table {}\n\
Run the scraper for {} seconds\n\
                ".format(self._search_for, self._lang, self._database_name,
                         self._database_table, self._limit)
                    return answer
                else:
                    answer = "\
Search for tweets containing the keywords {} that are in {}\n\
Place these tweets into the database {} in the table {}\n\
Collect {} tweets\n\
                    ".format(self._search_for, self._lang, self._database_name,
                             self._database_table, self._limit)
                    return answer
            else:
                if self._limit_type.lower() == "time":
                    answer = "\
Search for tweets containing the keywords {}\n\
Place these tweets into the database {} in the table {}\n\
Run the scraper for {} seconds\n\
                             ".format(self._search_for, self._database_name, self._database_table, self._limit)
                    return answer
                else:
                    answer = "\
Search for tweets containing the keywords {}\n\
Place these tweets into the database {} in the table {}\n\
Collect {} tweets\n\
                    ".format(self._search_for, self._database_name, self._database_table, self._limit)
                    return answer

    def search_for(self, terms=list()):
        """
        *IMPORTANT*
        If search_type is 'track' This will be a list of terms
        Otherwise, this will be a geolocation box

        :type terms: list
        :param terms: A :list: of terms, or a :list: containing a geolocation box
        :return: None
        """
        self._search_for = terms

    def set_limit_type(self, limit_type="count"):
        """
        Sets the type of limit to scrape up to.
        If set to count, the scraper will run until the 'limit' is reached on tweets
        If set to time, the scraper will scrape for 'limit' seconds

        :type limit_type: str
        :param limit_type: The limit type to impose
        :return: None
        """
        self._limit_type = limit_type

    def set_limit(self, limit):
        """
        Sets the limit, in either seconds or number of tweets to collect

        :type limit: int
        :param limit: The limit to scrape for
        :return: None
        """
        self._limit = limit

    def set_languages(self, languages=list()):
        """
        Sets the languages that will collected by the scraper

        :param languages: A :list: of languages to scrape for
        :return: None
        """
        self._lang = languages
    # TODO Expand out the location_lookup into it's own function

    def location_lookup(self, location_code):
        """
        :type location_code: str
        :param location_code: The location code to look up
        :return: None
        """
        self._location_lookup = location_code

    def name_database(self, database):
        """
        :type database: str
        :param database: The database to store things into
        :return:
        """
        self._database_name = database

    def name_table(self, table_name):
        """
        :type table_name: str
        :param table_name: The table to store the information in
        :return: None
        """
        self._database_table = table_name

    def set_geolocation_box(self, geobox=list()):
        """
        The specific coordinates of a geo-location box to scrape within

        :type geobox: list
        :param geobox: A :list: containing the 4 corners of a geolocation box
        :return: None
        """
        self._location_geo_box = geobox

    def scrape(self):
        """
        Calls the scraper which then scrapes for the specified terms and stores them into a database
        :return: None
        """
        if self._search_type == "track":
            go(search_for=self._search_for, search_type=self._search_type, limit_type=self._limit_type.upper(),
               limit=self._limit, table_name=self._database_table, database_name=self._database_name, lang=self._lang)
        elif self._search_type == "location":
            if self._location_lookup is not None:
                go(location_geo_box_lookup=self._location_lookup, search_type=self._search_type,
                   limit=self._limit, limit_type=self._limit_type.upper(), lang=self._lang,
                   table_name=self._database_table, database_name=self._database_name)
            else:
                go(search_for=self._search_for, search_type=self._search_type, limit_type=self._limit_type.upper(),
                   lang=self._lang, table_name=self._database_table, database_name=self._database_name)


if __name__ == "__main__":
    scraper = Scraper("track")
    scraper.search_for(["syria", "trump"])
    scraper.name_database("TestStuff")
    scraper.name_table("abc")
    scraper.set_limit_type("time")
    scraper.set_limit(10)
    scraper.set_languages(['en'])
    print(scraper)

    scraper.scrape()
    con = DataBaseSearch('TestStuff')
    table = Table(table="abc", rows=['plain_text'])
    search_for = Statement(selection='lang', parameters="'en'")
    table.commit_statement(search_for)
    item = con.get_DataFrame(table.query)
    print(item)