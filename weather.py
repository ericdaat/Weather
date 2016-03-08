import json, urllib
import pandas as pd


days_per_month = {'01':'31','02':'29','03':'31','04':'30','05':'31','06':'30','07':'31','08':'31','09':'30','10':'31','11':'30','12':'31'}

def load_sites(path, number):
	# path is a csv file containg latitude and longitude of various sites, indexed by their site_id
	# number is how many sites you want to load
	# returns a pandas DataFrame
	all_sites = pd.read_csv('all_sites.csv')
	df = all_sites[['SITE_ID','LAT','LNG']][:number]

	return df

def closest_airport(df, number=1):
	# df is the dataframe obtained from the load_sites method
	# number is how many closest airports do you want for each site
	# uses the wunderground api to get the closest airports based on their latitude and longitude
	# returns a list of dictionaries for site. The dictionaries contains the site_id, and the airports list
	airports = []
	for idx, series in df.iterrows():
		site_id = series[0]
		lat = series[1]
		lng = series[2]
		url = "http://api.wunderground.com/api/154cd1b7582011db/geolookup/q/%s,%s.json" %(lat,lng)

		response = urllib.urlopen(url)
		data = json.loads(response.read())

		airports_temp = []
		for item in data['location']['nearby_weather_stations']['airport']['station'][:number]:
			airports_temp.append(item)

		airports.append({'site_id':"%01d" %site_id, 'airports': airports_temp})

	return airports


def weather_for_airport(airports_list, months):
	# airports_list is the list of dictionaries obtained in the closest_airport method
	# months is how many months of weather do you want, starting from January
	# saves a file per airport 
	for item in airports_list:
		site_id = item['site_id']
		airports = item['airports']

		for airport in airports:
			airport_code = airport['icao']
			annual_weather = pd.DataFrame()

			for month,days in sorted(days_per_month.iteritems())[:months]:
				for i in range(1,int(days) + 1):
					url = 'http://api.wunderground.com/api/154cd1b7582011db/history_2012%s%02d/q/%s.json' %(month,i,airport_code)
					print url

					response = urllib.urlopen(url)
					data = json.loads(response.read())
					observations = data['history']['observations']

					daily_weather = pd.DataFrame(observations)
					daily_weather.drop(['heatindexi','heatindexm','date','metar'],axis=1, inplace=True)

					daily_weather['utcdate'] = daily_weather['utcdate'].map(lambda x: "%s/%s/%s %s:%s" %(x['mon'],x['mday'],x['year'],x['hour'],x['min']))

					annual_weather = annual_weather.append(daily_weather)

			path = '%s_%s_weather.csv' %(site_id,airport_code)
			annual_weather.to_csv(path)
			print "saved at %s" %path


if __name__ == '__main__':
	# take how many sites you want
	sites = load_sites('all_sites.csv',1)
	# find the closest airport (works with one for now) to your sites
	airports = closest_airport(sites)
	# get the weather for the airport for how many months you like, and export it to csv
	weather_for_airport(airports, 1)
