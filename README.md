# SXSW 2019 Scheduler

## Introduction

This project aimed to help a team of Banco do Brasil employees to select which lectures they should attend. The team was composed by a Data Scientist and an Innovation Specialist and had the objective of increasing knowledge and capturing new perspectives that could contribute to the planning of innovative solutions for Banco do Brasil.

The South by Southwest Conference & Festivals takes place in Austin, Texas, and in 2019 occurred between March 8 and 17. It featured more than 6,000 events, including lectures, shows, movies, etc.

Due to a large number of activities, it was necessary to select some of the lectures most related to the objective of the mission, given the specialties of each member of the team.

We chose to create a Machine Learning software, consisting of three steps:

* scraping data from the SXSW website

* a predictive model to rank the events

* a cost-based scheduler

The details of each step will be detailed in the following section.

## Scheduler Steps


### Web Scraping

Gets title, description, date, time, geolocation (Google Maps API), etc.

### Predictive Model

Based on nearly 100 events ranked by the user, ordered the other ones using a Random Forest Regressor that examined events' texts.

### Cost-based Scheduler

Creates two possible agendas, based on a cost function. This function used the events' ranks, type of badge, and the distance between each other.

## Conclusion

The Scheduler worked well, creating agendas with good suggestions and short distances between events. I hope you'll enjoy the code!