# coding=utf-8

import lxml.html
import urllib2
import ConfigParser
import csv
import os
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

def getContent(i, q):
    """This is the worker thread function.
    It processes items in the queue one after
    another.  These daemon threads go into an
    infinite loop, and only exit when
    the main thread ends.
    """
    while True:
        print '%s: Looking for the next url' % i
        url, csvFile = q.get()
        print (i, url)

        request = urllib2.Request(url, None, headers)
        page = urllib2.urlopen(request)

        doc = lxml.html.document_fromstring(page.read())
        doc.make_links_absolute(config.get('urls', 'absolute_url'))

        reviews = doc.xpath(config.get('xpath', 'reviews'))
        for idx, review in enumerate(reviews):
            title = doc.xpath(config.get('xpath', 'title').replace('{index}', str(idx)))

            if len(title) > 0:
                title = title[0][3:].encode('utf-8')
                date = doc.xpath(config.get('xpath', 'date').replace('{index}', str(idx)))
                date, time = date[0].split(" - ")
                bewertung = doc.xpath(config.get('xpath', 'bewertung').replace('{index}', str(idx)))[0]
                positive = doc.xpath(config.get('xpath', 'positive').replace('{index}', str(idx)))[0].encode('utf-8')
                negative = doc.xpath(config.get('xpath', 'negative').replace('{index}', str(idx)))[0].encode('utf-8')

                csvFile.writerow([title, date, time, positive, negative, bewertung])
                print (title, date, time, positive, negative, bewertung)

        q.task_done()


# Set up some global variables
num_fetch_threads = 2
enclosure_queue = Queue()

# Set up some threads to fetch the enclosures
for i in range(num_fetch_threads):
    worker = Thread(target=getContent, args=(i, enclosure_queue))
    worker.setDaemon(True)
    worker.start()

# try:
#     cnx = mysql.connector.connect(user='root', database='critics', password='root')
# except mysql.connector.Error as err:
#     if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
#         print("Something is wrong with your user name or password")
#     elif err.errno == errorcode.ER_BAD_DB_ERROR:
#         print("Database does not exists")
#     else:
#         print(err)
# else:
#     cursor = cnx.cursor()
#     cursor.execute('set profiling = 1')

out = csv.writer(open("data.csv", "w"), delimiter=',', quoting=csv.QUOTE_MINIMAL)

csv_header = 'Title,Date,Time,Positive,Negative,Bewertung'

header = csv_header.split(',')
out.writerow(header)

categories = [s for s in config.sections() if "category" in s]

for category in categories:

    print "Working..."

    def checkLink(a):
            linkText = a.text.encode('utf-8')
            print linkText
            return str(linkText) == str("»")

    request = urllib2.Request(url, None, headers)
    page = urllib2.urlopen(request)

    doc = lxml.html.document_fromstring(page.read())
    doc.make_links_absolute(config.get('urls', 'absolute_url'))

    reviews = doc.xpath(config.get('xpath', 'reviews'))

    next_page = filter(checkLink, doc.xpath(config.get('xpath', 'next_page')))
    if len(next_page) > 0:
        next_page_url = next_page[0].get('href')
        print next_page_url
        enclosure_queue.put((next_page_url, csvFile))

    enclosure_queue.put((config.get(category, 'category_url'), out))

    print '*** Main thread waiting'
    enclosure_queue.join()
    print '*** Done'

    # cursor.execute('set profiling = 0')