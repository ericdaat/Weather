import pandas as pd

all_sites = pd.read_csv('all_sites.csv')
df =  all_sites[['SITE_ID','AIRPORT']][:3]

for idx, series in df.iterrows():
	site_id = series[0]
	airport = series[1]

	weather_airport = pd.DataFrame()

	for i in range(1,2):
		url = 'http://www.wunderground.com/history/airport/%s/2012/1/%i/DailyHistory.html?format=1' %(airport,i)
		daily_csv = pd.read_csv(url)
		daily_csv.rename(columns={'DateUTC<br />':'DateUTC'}, inplace=True)
		daily_csv['DateUTC'] = daily_csv['DateUTC'].map(lambda x: str(x)[:-6])
		weather_airport = weather_airport.append(daily_csv)

	path = '%i_%s_weather.csv' %(site_id,airport)
	weather_airport.to_csv(path)