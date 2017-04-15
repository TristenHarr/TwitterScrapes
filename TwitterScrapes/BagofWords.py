from Scraper.DataPuller import Table, DataBaseSearch, Statement
import nltk
import random
from nltk.tokenize import word_tokenize
from datetime import date
# from nltk.corpus import stopwords, state_union
# from nltk.tokenize import PunktSentenceTokenizer

# documents = [(list(movie_reviews.words(fileid)), category) for category in movie_reviews.categories() for fileid in movie_reviews.fileids(category)]


con = DataBaseSearch('Zodiac')
table = Table(table="Signs", rows=['plain_text', 'followers', 'user_twitter_birthday', 'tweeted_time'])
search_for = Statement(selection='lang', parameters="'en'")
table.commit_statement(search_for)
item = con.get_DataFrame(table.query)

def date_conv(x):
    my_dates = {"Jan":1, "Feb":2, "Mar":3, "Apr":4, "May":5, "Jun":6, "Jul":7, "Aug":8, "Sep":9, "Oct":10, "Nov":11,
                "Dec":12}
    return [my_dates[x[4:7]],int(x[8:10]) , int(x[26:])]

def date_calulate(d1, d2):
    first = date(d1[2],d1[0], d1[1])
    second = date(d2[2], d2[0], d2[1])
    final = first - second
    return final.days

my_training_set = []
my_testing_set = []
my_set = set()
for stuff in range(100):   #item.count()['plain_text']
    thing = word_tokenize(item.iloc[stuff]['plain_text'])
    thing = list(map(lambda z: z.lower(), thing))
    my_set |= set(thing)
print(my_set)
my_dates = {"Jan":1, "Feb":2, "Mar":3,"Apr":4, "May":5, "Jun":6, "Jul":7, "Aug":8, "Sep":9, "Oct":10, "Nov":11,"Dec":12}
for stuff in range(75):
    thing = word_tokenize(item.iloc[stuff]['plain_text'])
    thing = list(map(lambda x:x.lower(), thing))
    start = {key: False for key in my_set}
    for itemabc in thing:
        if itemabc in start.keys():
            start[itemabc] = True
    rating = item.iloc[stuff]['followers']
    tweeted_time = item.iloc[stuff]['tweeted_time']
    user_age = item.iloc[stuff]['user_twitter_birthday']
    the_time = date_calulate(date_conv(tweeted_time), date_conv(user_age))
    if rating/the_time >= 3:
        popular = True
    else:
        popular = False
    my_training_set.append((start, popular))
    # print(start)    #Dictionary of Booleans
    # print(thing)    # List of words
    # print(popular)  # Popular or not

for stuff in range(75,100):
    thing = word_tokenize(item.iloc[stuff]['plain_text'])
    thing = list(map(lambda x:x.lower(), thing))
    start = {key:False for key in my_set}
    for itemabc in thing:
        if itemabc in start.keys():
            start[itemabc] = True
    rating = item.iloc[stuff]['followers']
    tweeted_time = item.iloc[stuff]['tweeted_time']
    user_age = item.iloc[stuff]['user_twitter_birthday']
    the_time = date_calulate(date_conv(tweeted_time), date_conv(user_age))
    if rating / the_time >= 1:
        popular = True
    else:
        popular = False
    my_testing_set.append((start, popular))

print(my_training_set)
print(my_testing_set)
classifier = nltk.NaiveBayesClassifier.train(my_training_set)
print("Naive Bayes Algo accuracy percentage:", (nltk.classify.accuracy(classifier, my_testing_set)))
classifier.show_most_informative_features(15)

