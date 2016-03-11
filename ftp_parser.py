import pandas as pd
import sys, os
from ftplib import FTP
import gzip
from math import cos, sqrt

def download_from_ftp(ftp_name, to_download):
	ftp = FTP(ftp_name)
	print ftp.login()
	ftp.cwd('/pub/data/noaa/2012')

	for filename in to_download:
		if not os.path.isfile('output/%s.csv' %filename):
			with open('output/%s.gz'%filename,'wb') as output_file:
				print 'trying to do download %s.gz' %filename
				try :
					ftp.retrbinary('RETR %s.gz' % filename, output_file.write)
				except :
					# TODO: improve this part, get another file if the one we want doesn't exist
					print '%s.gz : no such file' %filename
					os.remove('output/%s.gz'%filename)
		else :
			print 'file %s exists' %filename

	ftp.quit()


def distance(lat1, lon1, lat2, lon2):
    delta_lat = 110.574 * (lat1 - lat2)
    delta_long = 111.320 * (lon1 - lon2) * cos(delta_lat)

    return sqrt(delta_lat**2 + delta_long**2)


def find_closest_station(sites,stations):
	
	closest_stations = []

	for site_index, site in sites[:3].iterrows():
		dist = sys.maxint
		closest_station = None
		for station_index, station in stations.iterrows():
			tmp_dist = distance(site['LAT'],site['LNG'],station['LAT'],station['LON'])
			if tmp_dist < dist:
				dist = tmp_dist
				closest_station = station
			
		closest_stations.append(
			{'SITE_ID':site['SITE_ID'],
			'CLOSEST_STATION':closest_station['STATION NAME'],
			'USAF':closest_station['USAF'],
			'WBAN':closest_station['WBAN'],
			'distance':"%.2f"%dist})

	return closest_stations


def get_data(filename):

	input_path = 'output/%s.gz'%filename
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

	os.remove(input_path)


if __name__ == '__main__':

	stations = pd.read_csv('isd-history.csv')
	stations = stations[stations.CTRY == 'US']
	sites = pd.read_csv('all_sites.csv')

	closest_stations_dict = find_closest_station(sites,stations)
	closest_stations_df = pd.DataFrame(closest_stations_dict)
	closest_stations_df[['USAF', 'WBAN']] = closest_stations_df[['USAF', 'WBAN']].astype(str)

	sites_and_stations = sites.merge(closest_stations_df, on='SITE_ID', how='left')
	sites_and_stations.to_csv('sites_and_stations.csv', index=False)

	files_to_download = []
	for item in closest_stations_dict:
		filename = "%s-%s-2012" %(item['USAF'],item['WBAN'])
		files_to_download.append(filename)
		
	download_from_ftp('ftp.ncdc.noaa.gov',files_to_download)

	for file in os.listdir('output/'):
		if file.endswith(".gz"):
			get_data(file[:-3])
