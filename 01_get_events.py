#%%
import requests
import urllib
import pandas as pd
from bs4 import BeautifulSoup, NavigableString, Tag
import re
import numpy as np
from multiprocessing.dummy import Pool as ThreadPool
import googlemaps
from datetime import datetime
import time

#%%
# gets google maps authentication key 
with open('.secret', 'r') as f:
    API_KEY = f.read().strip('\n')
gmaps = googlemaps.Client(key=API_KEY)

#%%
# gets events' URL
def get_events_list(url):
    print(url)

    # gets an html page
    response = requests.get(url)

    # removes repeated special characters on the web page, making it easier to parse
    page = BeautifulSoup(response.text, 'html5lib')

    # finds sections with events descriptions
    events = page.find_all('div', class_='single-event')

    # iterates over the list, getting the events' details
    ls = []
    for event in events:
        ls.append('https://schedule.sxsw.com' + event['data-event-url'])
    
    return ls

#%%
# gets event details, such as: name, date, time, place, address, abstract and access level
def get_event_details(url):

    # waits for a second, to not harm the web site
    time.sleep(1)

    print(url)

    # gets an html page
    response = requests.get(url)

    # removes repeated special characters on the web page, making it easier to parse
    page = BeautifulSoup(response.text, 'html5lib')

    # gets event name and if it is a mentor session
    try:
        event = page.find('h1', class_='event-name').text
        is_mentor = 1 if 'Mentor' in event else 0
    except AttributeError:
        event = None

    # finds date and time
    try:
        date_time = page.find('div', class_='event-date').string
        day = get_day(date_time)
        start, end = get_times(date_time)            
    except AttributeError:
        day = None
        start = None
        end = None

    # finds place's name
    try:
        place = page.find('header', class_='venue-title')
        place_links = place.find_all('a')
        place_name = ' '.join([x.string for x in place_links])
    except AttributeError:
        place_name = None

    # finds address
    try:
        address = page.find('span', 'address').text + ' Austin, TX, EUA'
    except AttributeError:
        address = None

    # finds abstract
    try:
        body = page.find('div', class_='body')
        paragraphs = body.find_all('p')
        abstract = ' '.join([x.text for x in paragraphs]).replace('\n', ' ')
    except AttributeError:
        abstract = None

    # finds interactive badge info and access level: primary, secondary etc.
    #     the access levels were set as: primary = 0; secondary = 0.5; without access = 1.0 
    access_level = 1.0

    for tp in [('Secondary Entry:', 0.5), ('Primary Entry:', 0.0)]:    
        try:
            al = get_access_level(page.find(text=tp[0]).parent.parent.text)
            if al:
                access_level = tp[1]
                print(tp[0])
        except:
            pass

    return {'event': event,
            'day': day, 
            'start': start, 
            'end': end, 
            'place': place_name, 
            'address': address, 
            'abstract': abstract, 
            'access_level': access_level,
            'is_mentor' : is_mentor}   


#%%
# regex to find the day of month
def get_day(txt):
    re1='.*?'	# Non-greedy match on filler
    re2='((?:(?:[0-2]?\\d{1})|(?:[3][01]{1})))(?![\\d])'	
    rg = re.compile(re1+re2,re.IGNORECASE|re.DOTALL)
    m = rg.search(txt)
    try:
        day = m.group(1)
    except:
        day = None
    return day


#%%
# regex to find both start and end times of a event
def get_times(txt):
    re1='.*?'	# Non-greedy match on filler
    re2='((?:(?:[0-1][0-9])|(?:[2][0-3])|(?:[0-9])):(?:[0-5][0-9])(?::[0-5][0-9])?(?:\\s?(?:am|AM|pm|PM))?)'	# HourMinuteSec 1
    re3='.*?'	# Non-greedy match on filler
    re4='((?:(?:[0-1][0-9])|(?:[2][0-3])|(?:[0-9])):(?:[0-5][0-9])(?::[0-5][0-9])?(?:\\s?(?:am|AM|pm|PM))?)'	# HourMinuteSec 2
    rg = re.compile(re1+re2+re3+re4,re.IGNORECASE|re.DOTALL)
    m = rg.search(txt)
    try:
        start = m.group(1)
    except:
        start = None
    
    try:
        end = m.group(2)
    except:
        end = None

    return( (start, end) )


#%%
# regex to find interactive badge access level
def get_access_level(txt):
    re1='.*?'	# Non-greedy match on filler
    re2='(Interactive)'	# Word 1

    rg = re.compile(re1+re2,re.IGNORECASE|re.DOTALL)
    m = rg.search(txt)
    try:
        p = m.group(1)
        return True
    except:
        return False


#%%
# uses Google Maps API to get geolocation coordinates of a place, based on its address
def get_coordinates(gmaps, address):
    geocode_result = gmaps.geocode(address)
    coords = geocode_result[0]['geometry']['location']
    return( (coords['lat'], coords['lng']) )

#%%
# saves the time this program starts
ts_start = datetime.now()

#%%
# gets events list URLs by date
main_urls = ['https://schedule.sxsw.com/2019/03/' + '{:02d}'.format(x) + '/events' for x in range(8, 18)] 

#%%
# effectively gets events list (distributes the work between 8 threads to get the job done faster)
pool = ThreadPool(8)
result = pool.map(get_events_list, main_urls)

#%%
# waits the threads end and terminates them
pool.close()
pool.join()

#%%
# flats the list of lists
details_urls = [item for sublist in result for item in sublist]

#%%
# effectively gets events details
pool = ThreadPool(8)
result = pool.map(get_event_details, details_urls)

#%%
# waits the threads end and terminates them
pool.close()
pool.join()

#%%
# creates pandas dataframe and saves the result in a CSV file.
events_pd = pd.DataFrame(result)
events_pd.to_csv('data/events.csv', index=True, index_label='id', sep='|')

#%%
# gets unique addresses
addresses = list(set(events_pd['address'].tolist()))

#%%
# effectively gets the geolocation coordinates
ls = []
for address in addresses:
    try:
        lat, lon = get_coordinates(gmaps, address)
        d = {'address': address}
        d['latitude'] = lat
        d['longitude'] = lon
        ls.append(d)
        time.sleep(3)
    except:
        pass

#%%
# saves addresses
addresses_pd = pd.DataFrame(ls)
addresses_pd.to_csv('data/addresses.csv', index=False, sep='|')

#%%
# adds coordinates to events data
events_pd = events_pd.merge(addresses_pd, on='address')

#%%
# saves events data
events_pd.to_csv('data/events.csv', index=True, index_label='id', sep='|')

#%%
# saves end of execution time and prints time elapsed
ts_end = datetime.now()
delta = ts_end - ts_start
print(delta)
