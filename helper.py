import json

import requests

OMDB_API_URL_KEY = 'http://www.omdbapi.com/?t={}&apikey=8b006ad1'


def tuple_to_json(tup_list):
    """
    Takes tuple or list of tuple and return list of stringified json
    :param tup_list: row
    :return: list of stringified json
    """
    res = []
    if isinstance(tup_list, tuple):
        val = json.dumps(r'{{title: {}, released_year: {}, rating: {}, id: {}, genres: {}}}'.format(*tup_list))
        val = json.loads(val)
        res.append(val)
    else:
        for tup in tup_list:
            val = json.dumps(r'{{title: {}, released_year: {}, rating: {}, id: {}, genres: {}}}'.format(*tup))
            val = json.loads(val)
            res.append(val)
    return res


def search_web(title):
    """
    Search and extracts the information using OMDB's API
    :param title: Title of the movie
    :return: tuple of required fields
    """
    try:
        r = requests.get(OMDB_API_URL_KEY.format(title))
        r.raise_for_status()
        res = r.json()
        try:
            rating = float(res['imdbRating'].strip())
        except:
            rating = -1
        return res['Title'].strip().lower(), int(res['Year'][:4].strip()), rating, res['imdbID'].strip(), json.dumps(
            res['Genre'].lower().replace(' ', '').split(','))
    except Exception as e:
        raise e


def check_create_table(cursor):
    """
    Checks if table is created or not and creates if not already created
    :param cursor: sqlite3 cursor
    :return: None
    """
    cursor.execute('''SELECT name FROM sqlite_master WHERE type='table' AND name=?''', ('movies',))
    if cursor.fetchall():
        return
    cursor.execute(
        '''CREATE TABLE movies (title TEXT, released_year INTEGER, rating REAL, id TEXT PRIMARY KEY, genres TEXT)''')
