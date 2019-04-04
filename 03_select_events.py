#%%
import pandas as pd
import numpy as np
import itertools
import math
from multiprocessing import Manager, Pool, cpu_count
from functools import partial


#%% 
# converts time in minutes
def time_to_mins(time):
    h = str(time.strftime("%H:%M:%S")).split(':')
    m = int(h[0]) * 60 + int(h[1])
    return m    


# check if two events happen at the same time
def has_overbooking(a, b):
    return max(0, min(time_to_mins(a[1]), time_to_mins(b[1])) - 
                  max(time_to_mins(a[0]), time_to_mins(b[0])))


# standardizes values in a 0 to 1 range
def max_min(value, v_min, v_max):
    new_value = None

    if value is not None:
        new_value = (value - v_min) / (v_max - v_min)
        
    return new_value


# generates a schedule based on a shift
def scheduler(schedule, df):
    print(schedule)
    # events by shift
    ev = df.loc[(df['day'] == schedule['day']) & \
                (df['start'] >= pd.to_datetime(schedule['start']).time()) & \
                (df['start'] < pd.to_datetime(schedule['end']).time())]

    # generates events combinations
    combos = list(itertools.combinations(ev['id'].tolist(), 2))

    # removes overbooked combinations
    ls = []
    for combo in combos:
        # gets events data
        ev_1 = ev.loc[ev['id'] == combo[0]].to_dict('r')[0]
        ev_2 = ev.loc[ev['id'] == combo[1]].to_dict('r')[0]

        if not has_overbooking((ev_1['start'], 
                          ev_1['end']), 
                         (ev_2['start'], 
                          ev_2['end'])):
            d = {'day': schedule['day'], 'start': schedule['start'], 'event_1' : combo[0], 'event_2': combo[1]}

            # calculates combinations costs
            d['rank'] = ev_1['rank'] + ev_2['rank']
            d['distance'] = math.sqrt( (ev_1['latitude'] - ev_2['latitude']) ** 2 + (ev_1['longitude'] - ev_2['longitude']) ** 2 )
            d['access'] = ev_1['access'] + ev_2['access']
            ls.append(d)    
    return ls


#%% filenames
IN = 'ranking.csv'
OUT = 'primary_schedule.csv'
OUT2 = 'alternative_schedule.csv'

#%% ranked events
df = pd.read_csv('data/' + IN, sep='|')

#%% adjust data
df['start'] = pd.to_datetime(df['start']).dt.time
df['end'] = pd.to_datetime(df['end']).dt.time
df = df.loc[~(df['start'].isnull() | df['end'].isnull())]
df.loc[df['end'] == pd.to_datetime('00:00:00').time(), 'end'] = pd.to_datetime('23:59:59').time()
df = df.loc[df['is_mentor'] == 0]
df = df.loc[df['access'] < 1]
df['duration'] = df.apply(lambda x: time_to_mins(x['end']) - time_to_mins(x['start']), axis=1)

#%% sets shifts
days = [x for x in list(set(df['day'])) if str(x) != 'nan']
shifts = [['06:00:00', '13:00:00'], ['14:00:00', '19:00:00']]
schedule = []
for day in days:
    for shift in shifts:
        schedule.append({'day': day, 'start': shift[0], 'end': shift[1]})

#%% divides the scheduling trials between processes
cpus = cpu_count() - 2
with Pool(processes=cpus,) as pool:
    ls =  pool.map(partial(scheduler, df=df), schedule)
flat_list = [item for sublist in ls for item in sublist]
df_combos = pd.DataFrame(flat_list)

#%% standardizes cost data
v_min, v_max = (df_combos['rank'].min(), df_combos['rank'].max())
df_combos['rank'] = df_combos.apply(lambda x : \
                                          max_min(x['rank'], v_min, v_max), 
                                          axis=1)
v_min, v_max = (df_combos['access'].min(), df_combos['access'].max())
df_combos['access'] = df_combos.apply(lambda x : \
                                      max_min(x['access'], v_min, v_max), 
                                      axis=1)
v_min, v_max = (df_combos['distance'].min(), df_combos['distance'].max())
df_combos['distance'] = df_combos.apply(lambda x : \
                                         max_min(x['distance'], v_min, v_max), 
axis=1)

#%% total cost
df_combos['cost'] = df_combos['rank'] + \
                     df_combos['access']  + \
                     df_combos['distance']  
df_combos.sort_values(by=['day', 'start', 'cost'], inplace=True)
df_combos.to_csv('data/' + OUT, sep='|', index=False)

#%% first schedule
primary = df_combos.groupby(by=['day', 'start']).first()
df_primary = df.loc[df['id'].isin(primary['event_1'].tolist() + primary['event_2'].tolist())]
df_primary.sort_values(by=['day', 'start'], inplace=True)
df_primary.to_csv('data/' + OUT, sep='|', index=False)
print('\n\n==== Primary Schedule ====\n')
print(df_primary[['day', 'start', 'end', 'place', 'event']])

#%% second schedule
exclude = list(primary['event_1']) + list(primary['event_2'])
alternative = df_combos[(~df_combos['event_1'].isin(exclude)) & (~df_combos['event_2'].isin(exclude))].groupby(by=['day', 'start']).first()
df_alternative = df.loc[df['id'].isin(alternative['event_1'].tolist() + alternative['event_2'].tolist())]
df_alternative.sort_values(by=['day', 'start'], inplace=True)
df_alternative.to_csv('data/' + OUT2, sep='|', index=False)
print('\n\n==== Alternative Schedule ====\n')
print(df_alternative[['day', 'start', 'end', 'place', 'event']])