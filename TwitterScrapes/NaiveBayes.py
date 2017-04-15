from Scraper.DataPuller import DataBaseSearch, Table, Statement
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk import FreqDist
from datetime import date

class TwitterBayes(object):

    def __init__(self, database, table):
        self.database = database
        self.table = table
        self.con = DataBaseSearch(database)
        self.Table = Table(table=self.table, rows=['*'])
        self.query = Statement(selection='lang', parameters="'en'")
        self.Table.commit_statement(self.query)
        self.data = self.con.get_DataFrame(self.Table.query)
        self.corpus = open("corpus.txt".format(self.database, self.table), 'a', encoding='utf-8')


    def followers(self, row=None):
        if row is None:
            return self.data['followers']
        else:
            return row['followers']

    def favorites(self, row=None):
        if row is None:
            return self.data['favorites']
        else:
            return row['favorites']

    def hashtags(self, row=None):
        if row is None:
            return self.data['hashtags']
        else:
            return row['hashtage']

    def place(self, row=None):
        if row is None:
            return self.data['place']
        else:
            return row['place']

    def plain_text(self, row=None):
        if row is None:
            return self.data['plain_text']
        else:
            return row['plain_text']

    def location(self, row=None):
        if row is None:
            return self.data['tweet_location']
        else:
            return row['tweet_location']

    def twitter_birthday(self, row=None):
        if row is None:
            return self.data['user_twitter_birthday']
        else:
            return row['user_twitter_birthday']

    def tweet_time(self, row=None):
        if row is None:
            return self.data['tweeted_time']
        else:
            return row['tweeted_time']

    def user_description(self, row=None):
        if row is None:
            return self.data['user_description']
        else:
            return row['user_description']

    def user_name(self, row=None):
        if row is None:
            return self.data['user_name']
        else:
            return row['user_name']

    def create_corpus(self, corpora_size):
        for i in range(self.data.count()['plain_text']):
            self.corpus.write(self.data.iloc[i]['plain_text'].lower()+" ")
        self.corpus.close()
        corpus = open('corpus.txt', 'r', encoding='utf-8')
        freq_dist = FreqDist(next(corpus).split(" ")).most_common(corpora_size)
        corpus.close()
        clear = open('corpus.txt', 'w')
        clear.close()
        self.freq_dist = freq_dist
        return self.freq_dist

    def clean_freq_dist(self):
        rem_list = []
        for i,k in enumerate(self.freq_dist):
            if k[0] in stopwords.words('english'):
                rem_list.append(i)
        for item in rem_list:
            self.freq_dist.pop(item)
        return self.freq_dist




def popularity_ratio(database, table):
    stuff = TwitterBayes(database, table)
    # stuff = TwitterBayes('Training', 'US')
    followers = stuff.followers()
    birthday = stuff.twitter_birthday()
    today = stuff.tweet_time()
    print(today, birthday)
    test = stuff.create_corpus(5000)
    def date_conv(x):
        my_dates = {"Jan":1, "Feb":2, "Mar":3, "Apr":4, "May":5, "Jun":6, "Jul":7, "Aug":8, "Sep":9, "Oct":10, "Nov":11,
                    "Dec":12}
        return [my_dates[x[4:7]], int(x[8:10]), int(x[26:])]

    def date_calulate(d1, d2):
        first = date(d1[2], d1[0], d1[1])
        second = date(d2[2], d2[0], d2[1])
        final = first - second
        return final.days
    print(date_conv(birthday))

    popularity_ratio("Training", "US")

# con = DataBaseSearch('Zodiac')
# table = Table(table="Signs", rows=['plain_text', 'followers', 'user_twitter_birthday', 'tweeted_time'])
# search_for = Statement(selection='lang', parameters="'en'")
# table.commit_statement(search_for)
# item = con.get_DataFrame(table.query)