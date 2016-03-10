import json, urllib
import pandas as pd
import os.path

api_key = open('api_key.txt', 'r').read()
days_per_month = {'01':'31','02':'29','03':'31','04':'30','05':'31','06':'30','07':'31','08':'31','09':'30','10':'31','11':'30','12':'31'}

def load_sites(path, first, last):
	# path is a csv file containg latitude and longitude of various sites, indexed by their site_id
	# number is how many sites you want to load
	# returns a pandas DataFrame
	all_sites = pd.read_csv('all_sites.csv')
	df = all_sites[['SITE_ID','LAT','LNG']][first:last]

	return df

def closest_airports(df, number):
	# df is the dataframe obtained from the load_sites method
	# number is how many closest airports do you want for each site
	# uses the wunderground api to get the closest airports based on their latitude and longitude
	# returns a list of dictionaries for site. The dictionaries contains the site_id, and the airports list
	airports = []
	for idx, series in df.iterrows():
		site_id = series[0]
		lat = series[1]
		lng = series[2]
		url = "http://api.wunderground.com/api/%s/geolookup/q/%s,%s.json" %(api_key,lat,lng)

		response = urllib.urlopen(url)
		try:
			data = json.loads(response.read())
		except ValueError:
			print 'no airport data'
			break

		airports_temp = []
		for item in data['location']['nearby_weather_stations']['airport']['station'][:number]:
			airports_temp.append(item)

		airports.append({'site_id':"%01d" %site_id, 'airports': airports_temp})

	return airports

def weather_for_airport(airport_dict, months):
	# airports_list is the list of dictionaries obtained in the closest_airports method
	# months is how many months of weather do you want, starting from January
	# saves a file per airport 

	site_id = airport_dict['site_id']
	airport = airport_dict['airports']
	airport = airport_dict['airports'][0]
	airport_code = airport['icao']

	file_name = '%s_weather.csv' %(airport_code)

	if os.path.isfile(file_name):  
		# check if file doesn't exist already. If so, don't download the data again
		# note : if the data has been downloaded one first time for let's say a month, it would cause the data for the same airport
		# to not be downloaded again, even though we want a different time period. This will be fixed later on, but for now, our script will run 
		# for a full year everytime, so we don't need to think of merging files from different time periods yet. 
		print 'file %s exists' %file_name
	else :
		annual_weather = pd.DataFrame()
		for month,day in sorted(days_per_month.iteritems())[:months]:
			for i in range(1,int(day) + 1):
				url = 'http://api.wunderground.com/api/%s/history_2012%s%02d/q/%s.json' %(api_key,month,i,airport_code)
				
				response = urllib.urlopen(url)
				try:
					data = json.loads(response.read())

				except ValueError:
					print 'no weather data'
					continue

				print url
				observations = data['history']['observations']
				daily_weather = pd.DataFrame(observations)
				daily_weather.drop(['heatindexi','heatindexm','date','metar'],axis=1, inplace=True)
				daily_weather['utcdate'] = daily_weather['utcdate'].map(lambda x: "%s/%s/%s %s:%s" %(x['mon'],x['mday'],x['year'],x['hour'],x['min']))
				annual_weather = annual_weather.append(daily_weather)

		annual_weather.to_csv(file_name)
		print "saved at %s" %file_name


if __name__ == '__main__':
	# take how many sites you want
	sites = load_sites('all_sites.csv',0,10)
	# find the closest airport (works with one for now) to your sites
	airports = closest_airports(sites, 1)
	# get the weather for the airport for how many months you like, and export it to csv
	for airport in airports:
		weather_for_airport(airport, 12)
