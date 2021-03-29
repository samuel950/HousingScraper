from bs4 import BeautifulSoup as bs
from threading import Thread
import matplotlib.pyplot as plt
import threading
import sys
import os
import shutil
import queue
import requests
import time
import sqlite3
import logging
import logging.handlers
import numpy


class ListingCrawlThread (Thread):
    def __init__(self, threadID):
        Thread.__init__(self)
        self.threadID = threadID

    def run(self):
        _logger.info('Starting thread to get listings. Thread id: ' + str(self.threadID))
        getListingPages()
        _logger.info('Thread finished obtaining listings. Thread id: ' + str(self.threadID))
        _logger.info('Listing crawl thread working on processing listings now. Thread id: ' + str(self.threadID))
        global _obtainedListings
        _obtainedListings = True
        count = 0
        z = 0
        while isRunning(myThreads) or not _listingQ.empty():
            try:
                link = _listingQ.get(timeout=1)
                _logger.info('Thread #' + str(self.threadID) + ' is processing: ' + link)
                if link[0] == 'h':
                    count += 1
                    processHouse(self, link, None)
                else:
                    count += processApt(self, link[1:], None)
            except queue.Empty:
                continue
        print('Listing crawl thread trying to acquire count lock')
        _countLock.acquire()
        print('Listing crawl thread acquired lock')
        global _count
        _count += count
        _countLock.release()
        _logger.info('Listing crawl thread terminating. Thread id: ' + str(self.threadID))


class ListingProcessingThread (Thread):
    def __init__(self, threadID, ip):
        Thread.__init__(self)
        self.threadID = threadID
        self.proxy = {"https": ip}

    def run(self):
        _logger.info('Starting listing processing thread. Thread id: ' + str(self.threadID))
        count = 0
        while not _obtainedListings or not _listingQ.empty():
            try:
                link = _listingQ.get(timeout=1)
                _logger.info('Thread #' + str(self.threadID) + ' is processing: ' + link)
                if link[0] == 'h':
                    count += 1
                    processHouse(self, link, self.proxy)
                else:
                    count += processApt(self, link[1:], self.proxy)
            except queue.Empty:
                continue       
        _countLock.acquire()
        global _count
        _count += count
        _countLock.release()
        _logger.info('Listing processing thread terminating. Thread id: ' + str(self.threadID))


class SqlThread (Thread):
    def __init__(self, threadID):
        Thread.__init__(self)
        self.threadID = threadID

    def run(self):
        _logger.info('Starting SQL processing thread. Thread id: ' + str(self.threadID))
        try:
            conn = sqlite3.connect(_db)
            cursor = conn.cursor()
        except Exception as e:
            print(e)
            quit()
        while (isRunning(myThreads) or listingThread.is_alive()) or not _sqlQ.empty():
            try:
                data = _sqlQ.get(timeout=1)
                _logger.info('Running insert with: ' + str(data))
                cursor.execute(_sqlInsert, data)
            except queue.Empty:
                continue
        print('Sql thread trying to commit data')
        conn.commit()
        _logger.info('Sql thread terminating. Thread id: ' + str(self.threadID))


def logInit() -> logging.Logger:
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    handler = logging.handlers.RotatingFileHandler('logs/runtime.log', maxBytes=10*1024*1024, backupCount=5)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s: %(message)s", datefmt='%d-%b-%y %H:%M:%S')
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.addHandler(logging.StreamHandler(sys.stdout))
    return logger


def getWebpage():
    targetLocation = input('Enter a single ZIPCODE or a CITY and STATE separated by a space ie \'austin tx\','
                           ' \'new york ny\'.\nMake sure to properly spell city names, '
                           'for example (correct)new york instead of (incorrect)newyork.'
                           '\nInput desired location here ->')
    try:
        int(targetLocation)  # input is a zipcode
        return _zillow + 'homes/' + targetLocation + _zipcodeMod, str(targetLocation)
    except ValueError:
        location = targetLocation.replace(' ', ', ')
        return _zillow + targetLocation.replace(' ', '-') + '/rentals/', location


def getListingPages():
    pageNum = 2
    _logger.info('Obtaining listings starting at: ' + _baseLink)
    s = requests.Session()
    s.get('https://www.zillow.com/homes/for_rent/', headers=_headers)
    while True:  # obtain the first page
        time.sleep(_timeout)
        page = s.get(_baseLink, headers=_headers)
        if page.status_code == 200:
            break

    soup = bs(page.content, 'html.parser')
    houseLinks = soup.findAll('a', class_='list-card-img')
    for hl in houseLinks:
        _listingQ.put(hl['href'])
    nextBtn = soup.find('a', {'title': 'Next page'})
    if nextBtn is None:  # there is only one page of listings
        return
    currHref = nextBtn['href']
    while True:  # logic for going through each page of the listings on zillow
        while True:
            time.sleep(_timeout)
            page = s.get(_zillow + currHref, headers=_headers)
            if page.status_code == 200:
                _logger.info('Got page #' + str(pageNum))
                pageNum += 1
                break
        soup = bs(page.content, 'html.parser')
        houseLinks = soup.findAll('a', class_='list-card-img')
        for hl in houseLinks:
            _listingQ.put(hl['href'])
        # the link for the next page can be found from an 'a' tag that has a title of 'Next page'
        nextBtn = soup.find('a', {'title': 'Next page'})
        prevHref = currHref
        currHref = nextBtn['href']
        if currHref == prevHref:
            _logger.info("Finished obtaining listings")
            break
    global _obtainedPages
    _obtainedPages = True
    _logger.info('There were a total of ' + str(pageNum - 1) + ' pages obtained')


def processHouse(self, link: str, proxy):
    time.sleep(_timeout)
    while True:  # keep trying to obtain the relevant page
        if proxy is None:
            page = requests.get(link, headers=_headers)
        else:
            try:
                page = requests.get(link, headers=_headers, proxies=proxy)
            except Exception as e:
                print('Error! Thread #' + str(self.threadID) + ' ' + str(e))
                _listingQ.put(link)
                time.sleep(_timeout*2)  # let another thread have a chance to pick up work if proxy is failing often
                return
        if page.status_code == 200:
            break
        time.sleep(_timeout)
    soup = bs(page.content, 'html.parser')
    try:
        rent = soup.find('div', class_='ds-summary-row').findChild().findChild().contents[0].text
    except AttributeError as ae:
        print('Error! Thread #' + str(self.threadID) + ' ' + str(ae))
        _listingQ.put(link)
        time.sleep(_timeout*2)
        return
    # price = sub(r'[^\d]', '', price)  # regex version for converting currency amount to a 'normalised' number
    rent = rent.replace('$', '').replace(',', '').replace('+', '')  # converting dollar amount to a 'normalised' number
    bbl = soup.findAll('span', class_='ds-bed-bath-living-area')  # bed-bath-living area
    bedrooms = bbl[0].findChild().text
    bathrooms = bbl[1].findChild().text
    sqft = bbl[2].findChild().text.replace(',', '')
    address = soup.find('title').text.split(',')[0]
    street = address.lstrip("0123456789 ")
    if address == '(Undisclosed Address)' or bathrooms == '--' or bedrooms == '--' or rent == '--' or sqft == '--':  # missing info so skip
        return
    if bedrooms == 'Studio':
        bedrooms = 1
    _sqlQ.put([rent, bedrooms, bathrooms, sqft, address, street])
    # _conn.commit()  # using for debugging purposes, may remove this line


def processApt(self, link: str, proxy) -> int:
    count = 0
    time.sleep(_timeout)
    while True:  # keep trying to obtain the relevant page
        if proxy is None:
            page = requests.get(_zillow + link, headers=_headers)
        else:
            try:
                page = requests.get(_zillow + link, headers=_headers, proxies=proxy)
            except Exception as e:
                print('Error! Thread #' + str(self.threadID) + ' ' + str(e))
                _listingQ.put('/' + link)
                time.sleep(_timeout*2)  # let another thread have a chance to pick up work if proxy is failing often
                return 0
        if page.status_code == 200:
            break
        time.sleep(_timeout)
    soup = bs(page.content, 'html.parser')
    address = soup.find('h2', {'data-test-id': 'bdp-building-address'})
    if address is None:  # sometimes the address is located in a different html tag so we need this extra if statement
        address = soup.find('h1', {'data-test-id': 'bdp-building-title'})
    if address is None:
        print('Error! Thread #' + str(self.threadID) + ' Address is None which means there was trouble loading the page')
        _listingQ.put('/' + link)
        time.sleep(_timeout*2)  # let another thread have a chance to pick up work if proxy is failing often
        return 0
    address = address.text
    if ',' in address:
        address = address.split(',')[0]
    if address == '(Undisclosed)':  # skip listing since its missing vital data
        return 0
    street = address.lstrip("0123456789 ")
    floorplans = soup.findAll('div', class_='floorplan-info')
    if len(floorplans) == 0:
        floorplans = soup.findAll('div', class_='unit-card-grid unit-card__unit-info')
    for x in floorplans:
        # some apartment listings have hidden html elements that contain extra data that we do not care about
        if x.findParent('div', {'data-test-id': 'building-units-card-groups-container-for-sale'}) is None:
            rent = x.findChild().text
            if ' - ' in rent:
                rent = rent.split(' - ')[0]
            rent = rent.replace('$', '').replace(',', '').replace('+', '')
            bbl = x.findAll('span', class_='units-table__text--smallbody bdp-home-dna-val')
            bedrooms = bbl[0].text
            bathrooms = bbl[1].text
            sqft = bbl[2].text.replace(',', '')
            if bathrooms == '--' or bedrooms == '--' or rent == '--' or sqft == '--':  # missing info so skip
                break
            if bedrooms == 'Studio':
                bedrooms = 1
            count += 1
            _sqlQ.put([rent, bedrooms, bathrooms, sqft, address, street])
    return count


def statistics():
    try:
        cursor = sqlite3.connect(_db).cursor()
    except Exception as e:
        print(e)
        quit()
    cursor.execute("SELECT rent, sqft FROM listing")
    arr = numpy.fromiter(cursor.fetchall(), count=cursor.rowcount, dtype='i4,i4')
    arr2d = arr.view(numpy.int32).reshape((len(arr), -1))
    average = arr2d.mean(axis=0)
    average = numpy.around(average, decimals=2)
    slope = average[0]/average[1]
    x = arr2d[:,1]
    y = arr2d[:,0]
    xmax = max(x)+1
    ymax = max(y)+1
    z = numpy.random.random((arr2d.shape[0], 1))
    plt.scatter(x, y, s=5, c=z, cmap='prism')
    plt.ylim(ymin=0)
    plt.xlim(xmin=0)
    if _location is None:
        plt.title("Rent vs SQFT in Austin, TX\nAverage Rent: " + str(average[0]) + "\nAverage SQFT: " + str(average[1]))
    else:
        plt.title("Rent vs SQFT in " + _location)
    plt.xlabel("SQFT")
    plt.ylabel("Rent in USD")
    plt.xticks(range(0, xmax, 350))
    plt.yticks(range(0, ymax, 500))
    plt.axline((0, 0), slope=slope, color='black', label='average rent per sqft')
    plt.legend()
    plt.show()


def isRunning(threads):
    for t in threads:
        if t.is_alive():
            return True
    return False


def resetDB():
    _logger.info('Resetting sql database')
    os.remove(_db)
    shutil.copy('sqlbackup/skeleton.db', 'data.db')
    _logger.info('Quitting program after db reset')
    quit()


def emptyDB():
    _logger.info('Resetting sql database')
    os.remove(_db)
    shutil.copy('sqlbackup/skeleton.db', 'data.db')


_logger = logInit()
_logger.info('Starting program')
_user = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0'
_accept = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
_acceptEncoding = 'gzip, deflate, br'
_acceptLanguage = 'en-US,en;q=0.5'
_referer = 'https://www.zillow.com/homes/for_rent/'
_headers = {'User-Agent': _user,
            'Accept': _accept,
            'Accept-Encoding': _acceptEncoding,
            'Accept-Language': _acceptLanguage}
_zillow = 'https://www.zillow.com/'
_zipcodeMod = '_rb/'  # needed to create the correct link when searching a zipcode
_ips = ['http://208.80.28.208:8080', 'http://91.149.203.9:3128', 'http://159.65.69.186:9300',
        'http://74.208.31.248:80', 'http://3.17.188.25:80']  # may have to look up new ips every once in a while
_db = 'data.db'
_sqlInsert = ''' INSERT INTO listing(rent,bedrooms,bathrooms,sqft,address,street) VALUES(?,?,?,?,?,?) '''
_count = 0
_obtainedListings = False
_countLock = threading.Lock()
emptyDB()
_baseLink, _location = getWebpage()
_sqlQ = queue.Queue()
_listingQ = queue.Queue()
_timeout = 2
# ****** ABOVE IS SETUP ******
listingThread = ListingCrawlThread(1)
listingThread.start()
myThreads = []
numThreads = len(_ips)
for i in range(0, numThreads):
    thread = ListingProcessingThread(i+2, _ips[i])
    thread.start()
    myThreads.append(thread)
sqlThread = SqlThread(numThreads+1)
sqlThread.start()
for t in myThreads:
    t.join()
listingThread.join()
sqlThread.join()
_logger.info('Total number of rentals: ' + str(_count))
statistics()
_logger.info('Ending program')
