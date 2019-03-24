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
# en: get google maps authentication key 
# pt: chave de autenticacao do google maps
with open('.secret', 'r') as f:
    API_KEY = f.read().strip('\n')
gmaps = googlemaps.Client(key=API_KEY)

#%%
# en: get events' URL
# pt: recupera as urls de todos eventos
def get_events_list(url):
    print(url)

    # en: get an html page
    # pt: recupera pagina web
    response = requests.get(url)

    # en: remove repeated special characters on the web page, making it easier to parse
    # pt: retira caracteres especiais repetidos da pagina web, deixando-a mais legivel
    page = BeautifulSoup(response.text, 'html5lib')

    # en: find sections with events descriptions
    # pt: encontra trechos com a descricao dos events
    events = page.find_all('div', class_='single-event')

    # en:
    # pt: percorre a lista, obtendo detalhes dos events
    ls = []
    for event in events:
        ls.append('https://schedule.sxsw.com' + event['data-event-url'])
    
    return ls

#%%
# en: get event details, such as: name, date, time, place, address, abstract and access level
# pt: recupera detalhes do evento, tais como: nome, dia, horario, local, endereco, resumo e nivel de acesso
def get_event_details(url):

    # en: wait for a second, to not harm the web site
    # pt: espera por um segundo, para nao prejudicar o site
    time.sleep(1)

    print(url)

    # en: get an html page
    # pt: recupera pagina web
    response = requests.get(url)

    # en: remove repeated special characters on the web page, making it easier to parse
    # pt: retira caracteres especiais repetidos da pagina web, deixando-a mais legivel
    page = BeautifulSoup(response.text, 'html5lib')

    # en: get event name and if it is a mentor session
    # pt: recupera nome do evento e se eh de mentoria
    try:
        event = page.find('h1', class_='event-name').text
        is_mentor = 1 if 'Mentor' in event else 0
    except AttributeError:
        event = None

    # en: find date and time
    # pt: encontra dia e horario
    try:
        date_time = page.find('div', class_='event-date').string
        day = get_day(date_time)
        start, end = get_times(date_time)            
    except AttributeError:
        day = None
        start = None
        end = None

    # en: find place's name
    # pt: encontra nome do local
    try:
        place = page.find('header', class_='venue-title')
        place_links = place.find_all('a')
        place_name = ' '.join([x.string for x in place_links])
    except AttributeError:
        place_name = None

    # en: find address
    # pt: encontra endereco
    try:
        address = page.find('span', 'address').text + ' Austin, TX, EUA'
    except AttributeError:
        address = None

    # en: find abstract
    # pt: encontra resumo
    try:
        body = page.find('div', class_='body')
        paragraphs = body.find_all('p')
        abstract = ' '.join([x.text for x in paragraphs]).replace('\n', ' ')
    except AttributeError:
        abstract = None

    # en: find interactive badge info and access level: primary, secondary etc.
    #     the access levels were set as: primary = 0; secondary = 0.5; without access = 1.0 
    # pt: encontra marcacao de interactive badge e identifica nivel de acesso.
    #     os acessos sao os seguintes: primario = 0; secundario = 0.5; sem acesso = 1.0 
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
# en: regex to find the day of month
# pt: regex para encontrar o dia do mes
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
# en: regex to find both start and end times of a event
# pt: regex para encontrar os horarios de inicio e de fim de um evento
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
# en: regex to find interactive badge access level
# pt: regex para encontar o nivel de acesso para quem tem passaporte interactive
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
# en: uses Google Maps API to get geolocation coordinates of a place, based on its address
# pt: usa API do Google Maps para recuperar as coordenadas geográficas de cada local, baseado em seu endereco
def get_coordinates(gmaps, address):
    geocode_result = gmaps.geocode(address)
    coords = geocode_result[0]['geometry']['location']
    return( (coords['lat'], coords['lng']) )

#%%
# en: save the time this program starts
# pt: marca tempo de inicio de execucao
ts_start = datetime.now()

#%%
# en: get events list URLs by date
# pt: paginas de eventos por dia
main_urls = ['https://schedule.sxsw.com/2019/03/' + '{:02d}'.format(x) + '/events' for x in range(8, 18)] 

#%%
# en: effectively get events list (distribute the work between 8 threads to get the job done faster)
# pt: recuperar urls de detalhes dos events (distribui o trabalho em 8 threads para deixar essa etapa mais rapida)
pool = ThreadPool(8)
result = pool.map(get_events_list, main_urls)

#%%
# en: wait the threads finish and terminate them
# pt: esperar threads finalizarem e fechar pool
pool.close()
pool.join()

#%%
# en: flat the list of lists
# pt: achatar lista de listas em lista
details_urls = [item for sublist in result for item in sublist]

#%%
# en: effectively get events details
# pt: recuperar detalhes
pool = ThreadPool(8)
result = pool.map(get_event_details, details_urls)

#%%
# en: wait the threads finish and terminate them
# pt: esperar threads finalizarem e fechar pool
pool.close()
pool.join()

#%%
# en: create pandas dataframe and save the result in a CSV file.
# pt: criar dataframe pandas e salva-lo em um arquivo CSV.
events_pd = pd.DataFrame(result)
events_pd.to_csv('data/events.csv', index=True, index_label='id', sep='|')

#%%
# en: get unique addresses
# pt: recuperar enderecos unicos
addresses = list(set(events_pd['address'].tolist()))

#%%
# en: effectively get the  geolocation coordinates
# pt: recuperar as coordenadas geograficas
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
# en: save addresses
# pt: salva enderecos
addresses_pd = pd.DataFrame(ls)
addresses_pd.to_csv('data/addresses.csv', index=False, sep='|')

#%%
# en: add coordinates to events data
# pt: adiciona as coordenadas para os dados do evento
events_pd = events_pd.merge(addresses_pd, on='address')

#%%
# en: save events data
# pt: salvar informacao
events_pd.to_csv('data/events.csv', index=True, index_label='id', sep='|')

#%%
# en: save end of execution time and print time elapsed
# pt: salva fim do tempo de execucao e mostra tempo decorrido
ts_end = datetime.now()
delta = ts_end - ts_start
print(delta)
