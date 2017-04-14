from TwitterScrapes.ScrapeData import *
from TwitterScrapes.Charts.word_count import top_words

#
# scraper = Scraper("location")
# scraper.location_lookup("US")
# scraper.name_database("ADatabase")
# scraper.name_table("TheTable")
# scraper.set_limit_type("time")
# scraper.set_limit(10)
# scraper.set_languages(['en'])
# print(scraper)
# scraper.scrape()

con = DataBaseSearch('ADatabase')
table = Table(table="TheTable", rows=['plain_text', 'tweet_location', 'tweet'])
search_for = Statement(selection='lang', parameters="'en'")
table.commit_statement(search_for)
item = con.get_DataFrame(table.query)
top_words(item, word_count=50)
