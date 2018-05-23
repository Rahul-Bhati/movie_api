import json
import sqlite3

from bottle import get, run, post, request, json_dumps

from helper import tuple_to_json, search_web, check_create_table


@get('/movies/id/<movie_id>')
def get_by_id(movie_id):
    """
    Returns row from the movies database where id if found else returns a dict
    :param movie_id: Movie id
    :return: string/dict
    """
    with sqlite3.connect('movie.db') as conn:
        c = conn.cursor()
        c.execute('''SELECT * FROM movies WHERE id=?''', (movie_id,))
        res = c.fetchall()
        if res:
            res = tuple_to_json(res)
            return json_dumps(res)
        return {movie_id: "No movie found with give id"}


@get('/movies/year/<year>')
def get_by_year(year):
    """
    Queries movies database with specific year or range separated by - and returns information
    :param year: Year to be search or range of years
    :return: string/dict
    """
    with sqlite3.connect('movie.db') as conn:
        c = conn.cursor()
        n_year = map(int, year.split('-'))
        start_year = n_year[0]
        end_year = n_year[-1]
        c.execute('''SELECT * FROM movies WHERE released_year>=? AND released_year<=?''', (start_year, end_year,))
        res = c.fetchall()
        if res:
            res = tuple_to_json(res)
            return json_dumps(res)
        return {year: "No movie found for/between given year"}


@get('/movies/rating/<rating>')
def get_higher_or_lower(rating):
    """
    Queries movies database and returns movies with higher or lower rating or json message if movies not present
    :param rating: Rating of the movie
    :return: string/dict
    """
    with sqlite3.connect('movie.db') as conn:
        c = conn.cursor()
        c.execute('''SELECT * FROM movies WHERE rating!=?''', (float(rating),))
        res = c.fetchall()
        if res:
            res = tuple_to_json(res)
            return json_dumps(res)
        return {rating: "No movie with higher or lower than given year"}


@get('/movies/genre/<genre>')
def get_by_genre(genre):
    """
    Queries movies database and return queries with the matching genre
    :param genre: Genre to be matched
    :return: string/dict
    """
    genre = genre.lower()
    with sqlite3.connect('movie.db') as conn:
        c = conn.cursor()
        c.execute('''SELECT * FROM movies WHERE genres LIKE ?''', ('%{}%'.format(genre),))
        res = c.fetchall()
        if res:
            res = tuple_to_json(res)
            return json_dumps(res)
        return {genre: "No movie found with the given genre"}


@post('/movies/update/genres')
def update_genre():
    """
    Updates the genre in movie database for movie whose id is provided (Expects json post parameters)
    :return: string
    """
    with sqlite3.connect('movie.db') as conn:
        c = conn.cursor()
        movie_id = request.json.get('id').lower()
        new_genre = json.dumps(request.json.get('genres')).lower()
        c.execute('''SELECT * FROM movies WHERE id=?''', (movie_id,))
        if not c.fetchall():
            return "Please provide the correct movie id"
        c.execute('''UPDATE movies SET genres = ? WHERE id = ?''', (new_genre, movie_id,))
        return "Genre updated successfully"


@post('/movies/update/rating')
def update_rating():
    """
    Updates the rating of the movie whose id is provided (Expects json post parameters)
    :return: string
    """
    with sqlite3.connect('movie.db') as conn:
        c = conn.cursor()
        movie_id = request.json.get('id')
        rating = request.json.get('rating')
        c.execute('''SELECT * FROM movies WHERE id=?''', (movie_id,))
        if not c.fetchall():
            return "Please provide the correct movie id"
        c.execute('''UPDATE movies SET rating = ? WHERE id = ?''', (rating, movie_id,))
        return "Rating updated successfully"


@get('/movies/<title>')
def get_by_title(title):
    title = title.lower()
    with sqlite3.connect('movie.db') as conn:
        c = conn.cursor()
        check_create_table(c)
        try:
            c.execute('''SELECT * FROM movies WHERE title = ?''', (title,))
            res = c.fetchall()
            if res:
                res = tuple_to_json(res)
                return json_dumps(res)
        except Exception as e:
            raise e

        values = search_web(title)
        if values:
            try:
                print values
                c.execute('''INSERT INTO movies (title, released_year, rating, id, genres) VALUES (?, ?, ?, ?, ?)''',
                          values)
            except sqlite3.IntegrityError:
                print('ERROR: ID already exists in PRIMARY KEY id')
            try:
                res = tuple_to_json(values)
            except Exception as e:
                print e
            return res
        return {"Error": "Internal Server Error"}


if __name__ == '__main__':
    # with sqlite3.connect('movie.db') as conn:
    #     c = conn.cursor()
    #     # c.execute(
    #     #     '''CREATE TABLE movies (title TEXT, released_year INTEGER, rating REAL, id TEXT PRIMARY KEY, genres TEXT)''')
    #     # c.execute("INSERT INTO movies VALUES ('Test', '1990', '5', '123adbc', 'action')")
    #     # c.execute("INSERT INTO movies VALUES ('abc', '1990', '9', '123abxcdd', 'comedy')")
    #     c.execute('''SELECT * FROM movies''')
    #     for x in c.fetchall():
    #         print x
    run(reloader=True, debug=True)

    # print search_web('hello')
