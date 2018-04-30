from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
import folium
import folium.plugins as plugins
from datetime import datetime

def create_heatmap(sqlContext):
	specific_data = sqlContext.sql('SELECT Description, Date, Latitude, Longitude FROM crime_data')

	specific_data = specific_data.sample(False, .0001)
	heatmap_viz(specific_data)

def heatmap_viz(df):
	
	latitude = df.select(F.mean('Latitude')).collect()[0][0]
	longitude = df.select(F.mean('Longitude')).collect()[0][0]
	chicago_map = folium.Map(location=[latitude, longitude], 
 					zoom_start=10)


	points = df.rdd.map(lambda x: ((x.Description, x.Date), (x.Latitude, x.Longitude)))
	points = points.collect()

	points = sorted(points, key= lambda point: datetime.strptime(point[0][1], "%m/%d/%Y %I:%M:%S %p").year)

	final_points = []
	index = []
	for i in points:
		final_points.append([i[1][0], i[1][1]])
		index.append(datetime.strptime(i[0][1], "%m/%d/%Y %I:%M:%S %p").year)
		#time series not working

	plugins.HeatMap(final_points).add_to(chicago_map)
	chicago_map.save('time_heatmap.html')

