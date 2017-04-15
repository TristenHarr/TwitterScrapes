import sqlite3

def retrieval(password):
    conn = sqlite3.connect("serverside/secrets.sqlite").cursor()
    c = conn.execute("SELECT access_token, access_token_secret, consumer_key, consumer_secret FROM keys WHERE id IS"
                     " '{test}'".format(test=password))
    values = c.fetchone()
    c.close()
    return values
