import requests
import json

#Only avoids Fuller park currently can add interactive functionality with more time
#Chose fuller park since from our study it is the most dangerous area in chicago

#Enter these values to change routing
lat1 = 41.8046
lon1 = -87.6323
lat2 = 41.7943
lon2 = -87.63244

url = 'https://private-anon-d818fee2b4-openrouteservice.apiary-proxy.com/directions?api_key=58d904a497c67e00015b45fc377e1774d1d742c5a8547cc9674c465e&coordinates=' + str(lon1) + '%2C' + str(lat1) + '%7C' + str(lon2) + '%2C' + str(lat2) + '&profile=driving-car&units=mi&options=%7B%20%20%20%20%20%20%22avoid_polygons%22%3A%20%7B%20%20%20%20%20%22type%22%3A%20%22Polygon%22%2C%20%20%20%20%20%22coordinates%22%3A%20%5B%20%5B%20%5B-87.64514%2C41.80862%20%5D%2C%20%5B-87.63244%2C41.80862%20%5D%2C%20%5B-87.63244%2C41.79403%20%5D%2C%20%20%5B-87.64514%2C41.79403%20%5D%2C%20%5B-87.64514%2C41.80862%20%5D%5D%2C%20%5B%5B-87.63055%2C%2041.80906%5D%2C%20%5B-87.61716%2C%2041.80906%5D%2C%20%5B-87.61716%2C%2041.7948%5D%2C%20%5B-87.61639%2C%2041.7948%5D%2C%20%5B-87.63055%2C%2041.80906%5D%20%5D%20%20%20%5D%7D%20%7D&id=id&options=%7B%20%20%20%20%20%20%22avoid_polygons%22%3A%20%7B%20%20%20%20%20%22type%22%3A%20%22Polygon%22%2C%20%20%20%20%20%22coordinates%22%3A%20%5B%20%5B%20%5B-87.64514%2C41.80862%20%5D%2C%20%5B-87.63244%2C41.80862%20%5D%2C%20%5B-87.63244%2C41.79403%20%5D%2C%20%20%5B-87.64514%2C41.79403%20%5D%2C%20%5B-87.64514%2C41.80862%20%5D%5D%2C%20%5B%5B-87.63055%2C%2041.80906%5D%2C%20%5B-87.61716%2C%2041.80906%5D%2C%20%5B-87.61716%2C%2041.7948%5D%2C%20%5B-87.61639%2C%2041.7948%5D%2C%20%5B-87.63055%2C%2041.80906%5D%20%5D%20%20%20%5D%7D%20%7D'
url2 = 'https://private-anon-d818fee2b4-openrouteservice.apiary-proxy.com/directions?api_key=58d904a497c67e00015b45fc377e1774d1d742c5a8547cc9674c465e&coordinates=' + str(lon1) + '%2C' + str(lat1) + '%7C' + str(lon2) + '%2C' + str(lat2) + '&profile=driving-car&units=mi'


r = requests.get(url)
r2 = requests.get(url2)

json_val1 = r.json()
json_val2 = r2.json()

important_json = json_val1['routes'][0]['segments']

total_time = important_json[0]['duration']
total_distance = important_json[0]['distance']

total_steps = len(important_json[0]['steps'])
steps = important_json[0]['steps']

f = open('route_comparison.txt', 'w')
f.write("Route with avoiding\n")
f.write("===================\n")
f.write("Total time of trip:\t" + str(total_time) + ' seconds.\n')
f.write( "Total distance of trip:\t" + str(total_distance) + ' miles.\n')
f.write( "Total steps of trip:\t" + str((total_steps)) + ' steps.\n')

counter = 1
for i in steps:
	if "Arrive" not in i['instruction']:
		f.write( str(counter) + '. ' + i['instruction'] + ' for ' + str(i['distance']) + ' miles (' + str(i['duration']) + ' seconds).\n') 
	else:
		f.write( str(counter) + '. ' + i['instruction'] + '.\n\n\n')

	counter += 1

f.write("Route without avoiding\n")
f.write("======================\n")
important_json = json_val2['routes'][0]['segments']

total_time_2 = important_json[0]['duration']
total_distance_2 = important_json[0]['distance']

total_steps_2 = len(important_json[0]['steps'])
steps_2 = important_json[0]['steps']

f.write("Total time of trip:\t" + str(total_time_2) + ' seconds.\n')
f.write( "Total distance of trip:\t" + str(total_distance_2) + ' miles.\n')
f.write( "Total steps of trip:\t" + str((total_steps_2)) + ' steps.\n')

counter = 1
for i in steps_2:
	if "Arrive" not in i['instruction']:
		f.write( str(counter) + '. ' + i['instruction'] + ' for ' + str(i['distance']) + ' miles (' + str(i['duration']) + ' seconds).\n') 
	else:
		f.write( str(counter) + '. ' + i['instruction'] + '.\n\n\n')

	counter += 1

f.write("Comparison Statistics\n")
f.write("=====================\n")
f.write("Avoiding took " + str(total_time - total_time_2) + " more seconds.\n")
f.write("Avoiding took " + str(total_distance - total_distance_2) + " more miles.\n")
if(total_steps - total_steps_2 >= 0):
	f.write("Avoiding took " + str(total_steps - total_steps_2) + " more steps.\n")
else:
	f.write("Avoiding took " + str(total_steps_2 - total_steps) + " less steps.")


