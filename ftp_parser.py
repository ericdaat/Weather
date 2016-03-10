import pandas as pd
import sys, os
from ftplib import FTP
import gzip
from math import cos, asin, sqrt

def download_from_ftp(ftp_name, to_download):
	ftp = FTP(ftp_name)
	print ftp.login()
	ftp.cwd('/pub/data/noaa/2012')

	for filename in to_download:
		with open('output/%s.gz'%filename ,'wb') as output_file:
			try :
				ftp.retrbinary('RETR %s' % filename, output_file.write)
			except :
				# TODO: improve this part, get another file if the one we want doesn't exist
				print '%s.gz : no such file' %filename
	
	ftp.quit()


def distance(lat1, lon1, lat2, lon2):
    p = 0.017453292519943295
    a = 0.5 - cos((lat2 - lat1) * p)/2 + cos(lat1 * p) * cos(lat2 * p) * (1 - cos((lon2 - lon1) * p)) / 2
    return 12742 * asin(sqrt(a))


def find_closest_station(sites,stations):
	dist = sys.maxint
	closest_stations = []

	for site_index, site in sites[:1].iterrows():
		for station_index, station in stations[:3].iterrows():

			tmp_dist = distance(site['LAT'],site['LNG'],station['LAT'],station['LON'])
			if tmp_dist < dist:
				dist = tmp_dist
			
		closest_stations.append({'SITE_ID':site['SITE_ID'],'USAF':station['USAF'],'WBAN':station['WBAN'],'distance':"%.2f"%dist})

	return closest_stations


def get_data(filename):

	input_path = 'output/%s.gz'%filename
	if os.path.isfile(input_path):
		output_path = 'output/%s.csv'%filename

		with gzip.open(input_path, 'rb') as input_file, open(output_path, 'w') as output_file:
			output_file.write('time,lat,lng,wind_angle,wind_speed,air_temp\n')
			for line in input_file:
				time = line[15:27]
				lat = line[28:34]
				lng = line[34:41]
				wind_angle = line[60:63]
				wind_speed = line[65:69]
				air_temp = line[87:92]
				output_file.write('%s,%s,%s,%i,%i,%.2f\n' %(time,lat,lng,int(wind_angle),int(wind_speed),int(air_temp)/10.))

		os.remove('output/%s.gz'%filename)


if __name__ == '__main__':

	stations = pd.read_csv('isd-history.csv')
	stations = stations[stations.CTRY == 'US']
	sites = pd.read_csv('all_sites.csv')

	closest_stations_dict = find_closest_station(sites,stations)
	closest_stations_df = pd.DataFrame(closest_stations_dict)
	sites_and_stations = sites.merge(closest_stations_df, on='SITE_ID', how='left')
	sites_and_stations.to_csv('sites_and_stations.csv')

	files_to_download = []
	for item in closest_stations_dict:
		filename = "%s-%s-2012" %(item['USAF'],item['WBAN'])
		files_to_download.append(filename)
		
	download_from_ftp('ftp.ncdc.noaa.gov',files_to_download)

	for item in files_to_download:
		get_data(item)
