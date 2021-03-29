from bs4 import BeautifulSoup as bs
import matplotlib.pyplot as plt
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
    targetLocation = input('Enter a single ZIPCODE or a CITY and STATE separated by a space ie \'austin texas\','
                           ' \'new york new york\'.\nMake sure to properly spell city names, '
                           'for example (correct)new york instead of (incorrect)newyork.'
                           '\nInput desired location here ->')
    try:
        int(targetLocation)  # input is a zipcode
        return _zillow + 'homes/' + targetLocation + _zipcodeMod, str(targetLocation)
    except ValueError:
        location = targetLocation.replace(' ', ', ')
        return _zillow + targetLocation.replace(' ', '-') + '/rentals/', location
        # do other stuff


def getListingPages():
    pageNum = 2
    _logger.info('Obtaining listings starting at: ' + _baseLink)
    s = requests.Session()
    s.get('https://www.zillow.com/homes/for_rent/', headers=_headers)
    while True:  # obtain the first page
        time.sleep(_timeout)
        page = s.get(_baseLink, headers=_headers)
        if page.status_code == 200:
            _pageQ.put(page)
            # return # for debugging purposes
            break

    soup = bs(page.content, 'html.parser')
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
                _pageQ.put(page)
                break
        soup = bs(page.content, 'html.parser')
        # the link for the next page can be found from an 'a' tag that has a title of 'Next page'
        nextBtn = soup.find('a', {'title': 'Next page'})
        prevHref = currHref
        currHref = nextBtn['href']
        if currHref == prevHref:
            _logger.info("Finished obtaining listings")
            break
    _logger.info('There were a total of ' + str(pageNum - 1) + ' pages obtained')


def processHouse(link: str):
    time.sleep(_timeout)
    while True:  # keep trying to obtain the relevant page
        page = requests.get(link, headers=_headers)
        if page.status_code == 200:
            break
        time.sleep(_timeout)
    soup = bs(page.content, 'html.parser')
    rent = soup.find('div', class_='ds-summary-row').findChild().findChild().contents[0].text
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
    _cursor.execute(_sqlInsert, [rent, bedrooms, bathrooms, sqft, address, street])
    _conn.commit()  # using for debugging purposes, may remove this line


def processApt(link: str) -> int:
    count = 0
    time.sleep(_timeout)
    while True:  # keep trying to obtain the relevant page
        page = requests.get(_zillow + link, headers=_headers)
        if page.status_code == 200:
            break
        time.sleep(_timeout)
    soup = bs(page.content, 'html.parser')
    address = soup.find('h2', {'data-test-id': 'bdp-building-address'})
    if address is None:  # sometimes the address is located in a different html tag so we need this extra if statement
        address = soup.find('h1', {'data-test-id': 'bdp-building-title'})
    address = address.text
    if ',' in address:
        address = address.split(',')[0]
    if address == '(Undisclosed)': # skip listing since its missing vital data
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
            _cursor.execute(_sqlInsert, [rent, bedrooms, bathrooms, sqft, address, street])
            _conn.commit()
    return count


def statistics():
    _cursor.execute("SELECT rent, sqft FROM listing")
    arr = numpy.fromiter(_cursor.fetchall(), count=_cursor.rowcount, dtype='i4,i4')
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


def resetDB():
    _logger.info('Resetting sql database')
    os.remove(_db)
    shutil.copy('sqlbackup/skeleton.db', 'data.db')
    _logger.info('Quitting program after db reset')
    quit()


_logger = logInit()
_logger.info('Starting program')
_user = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/534.30 (KHTML, like Gecko) ' \
        'Ubuntu/11.04 Chromium/12.0.742.112 Chrome/12.0.742.112 Safari/534.30'
_referer = 'https://www.zillow.com/homes/for_rent/'
_headers = {'User-Agent': _user}
_zillow = 'https://www.zillow.com/'
_zipcodeMod = '_rb/'  # needed to create the correct link when searching a zipcode
_timeout = 2
_db = 'data.db'
_conn = None
_cursor = None
_sqlInsert = ''' INSERT INTO listing(rent,bedrooms,bathrooms,sqft,address,street) VALUES(?,?,?,?,?,?) '''
_location = None
reset = True
doMath = False
if reset is True:
    resetDB()  # reset db for testing purposes
try:
    _conn = sqlite3.connect(_db)
    _cursor = _conn.cursor()
except Exception as e:
    print(e)
    quit()
if doMath is True:  # for debugging purposes
    statistics()
    quit()
_baseLink, _location = getWebpage()
# ****** SETTING UP ******
_pageQ = queue.Queue()
getListingPages()
count = 0
while not _pageQ.empty():
    soup = bs(_pageQ.get().content, 'html.parser')
    # link to individual listing can be found from an 'a' tag that has a class of 'list-card-img'
    houseLinks = soup.findAll('a', class_='list-card-img')
    for a in houseLinks:
        _logger.info("Processing - " + a['href'])
        if a['href'][0] == 'h':
            count += 1
            processHouse(a['href'])
        else:
            count += processApt(a['href'][1:])
statistics()
_logger.info('Total number of rentals: ' + str(count))
_logger.info('Ending program')
