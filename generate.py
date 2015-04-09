# coding=utf-8

import lxml.html
import urllib2
import ConfigParser
import mysql.connector
import re
import time
from mysql.connector import errorcode
import __future__

# System modules
from Queue import Queue
from threading import Thread

config = ConfigParser.RawConfigParser()
config.read('settings.cfg')

user_agent = 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'
headers = {'User-Agent': user_agent}


def transformFilmRatin(rating):
    try:
        rating = rating.lower().replace(" ", "").replace("'", "")
        if "of" in rating:
            return transformFilmRatin(rating.replace("of", "/"))
        if "/" in rating:
            if len(rating.split("/")[-1]) == 0:
                rating += "4"
            rating = re.sub("\D\.", "", rating)
            return eval(compile(rating, '<string>', 'eval', __future__.division.compiler_flag)) * 100
        elif "." in rating:
            return float(rating) / 4.0 * 100
        elif re.search('[a-zA-Z]', rating):
            if rating == "a+":
                return 100
            elif rating == "a":
                return 90
            elif rating == "a-":
                return 80
            elif rating == "b+":
                return 80
            elif rating == "b":
                return 70
            elif rating == "b-":
                return 60
            elif rating == "c+":
                return 50
            elif rating == "c":
                return 40
            elif rating == "c-":
                return 30
            elif rating == "d+":
                return 20
            elif rating == "d":
                return 10
            elif rating == "d-":
                return 0
            elif rating == "f":
                return 0
            else:
                print "|||ERROR||| Cannot interpretate rating: %s" % rating
    except:
        print "|||ERROR||| Cannot interpretate rating: %s" % rating
        return 0


def getContent(i, q, cnx, cursor):
    """This is the worker thread function.
    It processes items in the queue one after
    another.  These daemon threads go into an
    infinite loop, and only exit when
    the main thread ends.
    """
    while True:
        print '%s: Looking for the next url' % i
        queue_data = q.get()

        if len(queue_data) == 3:
            url, is_tomatometer_approved, is_top_critic = queue_data
            print (i, url, is_tomatometer_approved, is_top_critic)

            request = urllib2.Request(url, None, headers)
            page = urllib2.urlopen(request)

            doc = lxml.html.document_fromstring(page.read())
            doc.make_links_absolute(config.get('urls', 'absolute_url'))

            title = doc.xpath(config.get('xpath', 'title'))
            title = title[0].encode('utf-8').strip()

            # print (title, url)

            createCriticQuery = """INSERT INTO critics (name, tomatometer_approved, top_critic, ordinary_critic) VALUES (%s, %s, %s, %s)"""

            try:
                cursor.execute(createCriticQuery, (title,
                                                   str(int(is_tomatometer_approved)),
                                                   str(int(is_top_critic)),
                                                   str(int(not (is_tomatometer_approved or is_top_critic)))))
                cnx.commit()

                critic_id = cursor.lastrowid
            except:
                cnx.rollback()
                cnx.commit()

                selectCritic = """SELECT `id` FROM `critics` WHERE `name` = %s"""
                cursor.execute(selectCritic, (title,))

                results = cursor.fetchall()
                if len(results) > 0:
                    critic_id = results[0][0]
                else:
                    print "%s: |||ERROR||| No critic found for name: %s" % (i, str(title))
                    break

            film_titles = doc.xpath(config.get('xpath', 'film_title').replace('[{index}]', ''))

            for idx, film in enumerate(film_titles):
                film_rating = doc.xpath(config.get('xpath', 'film_rating').replace('[{index}]', '[' + str(idx + 1) + ']'))

                film = film.encode('utf-8').strip()

                if len(film_rating) is 1:
                    film_rating = film_rating[0]
                    if len(film_rating) > 0:
                        rating_number = transformFilmRatin(film_rating)

                        createFilmQuery = """INSERT INTO `films` (`title`) VALUES (%s)"""

                        film_id = ''
                        try:
                            cursor.execute(createFilmQuery, (str(film),))
                            cnx.commit()

                            film_id = cursor.lastrowid
                        except:
                            cnx.rollback()
                            cnx.commit()

                            selectFilmQuery = """SELECT `id` FROM `films` WHERE `title` = %s"""

                            cursor.execute(selectFilmQuery, (str(film),))

                            results = cursor.fetchall()
                            if len(results) > 0:
                                film_id = results[0][0]
                            else:
                                print "%s: |||ERROR||| No film found for title: %s" % (i, str(title))
                                break

                        createRatingQuery = """INSERT INTO `critics_films` (`critic_id`, `film_id`, `rating`) VALUES (%s, %s, %s)"""
                        try:
                            cursor.execute(createRatingQuery, (critic_id, film_id, rating_number))
                            cnx.commit()
                        except:
                            cnx.rollback()
                            updateQuery = """UPDATE `critics_films` SET `rating`=%s WHERE `critic_id`=%s AND `film_id`=%s"""
                            cursor.execute(updateQuery, (critic_id, film_id, rating_number))

            next_url = doc.xpath(config.get('xpath', 'next_films'))

            if len(next_url) > 0:
                print 'Queuing:', next_url[0]
                enclosure_queue.put((next_url[0], is_tomatometer_approved, is_top_critic))

            q.task_done()
        else:
            print "%s: |||ERROR||| Queue : %s" % (i, queue_data)
            q.task_done()

# Set up some global variables
num_fetch_threads = 50
enclosure_queue = Queue()

# Set up some threads to fetch the enclosures
for i in range(num_fetch_threads):
    cnx = mysql.connector.connect(user='root', database='critics', password='root')
    worker = Thread(target=getContent, args=(i, enclosure_queue, cnx, cnx.cursor()))
    worker.setDaemon(True)
    worker.start()

try:
    cnx = mysql.connector.connect(user='root', database='critics', password='root')
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exists")
    else:
        print(err)
else:
    cursor = cnx.cursor()
    cursor.execute('set profiling = 1')
    categories = [s for s in config.sections() if "category" in s]

    for category in categories:
        category_image = ''
        print "Working..."
        request = urllib2.Request(config.get(category, 'category_url'), None, headers)
        parent_page = urllib2.urlopen(request)

        parent_doc = lxml.html.document_fromstring(parent_page.read())
        parent_doc.make_links_absolute(config.get('urls', 'absolute_url'))

        root_urls = parent_doc.xpath(config.get('xpath', 'root_urls'))
        print "Finding root urls..."
        for root_url in root_urls:
            request = urllib2.Request(root_url, None, headers)
            parent_page = urllib2.urlopen(request)

            parent_doc = lxml.html.document_fromstring(parent_page.read())
            parent_doc.make_links_absolute(config.get('urls', 'absolute_url'))

            urls = parent_doc.xpath(config.get('xpath', 'urls'))

            tomatometer_approved_request = urllib2.Request(root_url.replace('view=3', 'view=1'))
            tomatometer_approved_page = urllib2.urlopen(tomatometer_approved_request)
            tomatometer_approved_doc = lxml.html.document_fromstring(tomatometer_approved_page.read())
            tomatometer_approved_doc.make_links_absolute(config.get('urls', 'absolute_url'))

            tomatometer_approved_urls = tomatometer_approved_doc.xpath(config.get('xpath', 'urls'))

            top_critics_request = urllib2.Request(root_url.replace('view=3', 'view=2'))
            top_critics_page = urllib2.urlopen(top_critics_request)
            top_critics_doc = lxml.html.document_fromstring(top_critics_page.read())
            top_critics_doc.make_links_absolute(config.get('urls', 'absolute_url'))

            top_critics_urls = top_critics_doc.xpath(config.get('xpath', 'urls'))

            print "Finding critic urls..."
            for idx, url in enumerate(urls):
                print 'Queuing:', url
                enclosure_queue.put((url, url in tomatometer_approved_urls, url in top_critics_urls))
                # getContent(url, cursor, cnx, url in tomatometer_approved_urls, url in top_critics_urls)

        print '*** Main thread waiting'
        enclosure_queue.join()
        print '*** Done'

    cursor.execute('set profiling = 0')