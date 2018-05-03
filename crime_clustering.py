from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.feature import VectorAssembler
import folium
from folium.plugins import MarkerCluster
from datetime import datetime


def create_clusters(sqlContext):
	specific_data = sqlContext.sql('SELECT Description, Date, Latitude, Longitude FROM crime_data')
	#where `Primary Type` == "HOMOCIDE" or `Primary Type` == "ASSAULT" 
	specific_data.filter(specific_data.Description != "SIMPLE")

	specific_data =  specific_data.rdd.filter(lambda x: datetime.strptime(x[1], "%m/%d/%Y %I:%M:%S %p").weekday() in [5, 6]).toDF()


	specific_data = specific_data.sample(False, .0001)
	#create different k-means outputs depending on the date since that is what we are testing

	vecAssembler = VectorAssembler(inputCols=['Latitude', 'Longitude'], outputCol="features")
	df_kmeans = vecAssembler.transform(specific_data).select('Description', 'features', 'Latitude', 'Longitude')
	# df_kmeans.show()

	# cost = [0] * 40
	# for k in range(41,50):
	#     kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
	#     model = kmeans.fit(df_kmeans.sample(False,0.1, seed=42))
	#     cost[k-41] = model.computeCost(df_kmeans) # requires Spark 2.0 or later
	# print (cost)

	# 48 for k since this is where it starts to have diminishing returns
	k = 48
	kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
	model = kmeans.fit(df_kmeans)
	centers = model.clusterCenters()

    # print("Cluster Centers: ")
    # for center in centers:
    #     print(center)

	#TODO: arrange based on season, time of day, etc
	transformed = model.transform(df_kmeans).select('Description', 'prediction', 'Latitude', 'Longitude')
	transformed.show()

	#return_rdd = transformed.rdd.map(lambda x: (x.prediction, (x.Description, (x.Latitude, x.Longitude))))

	create_visuals(transformed)


	#TODO: weekday crimes vs weekend crimes

def create_visuals(df):
	
	latitude = df.select(F.mean('Latitude')).collect()[0][0]
	longitude = df.select(F.mean('Longitude')).collect()[0][0]
	chicago_map = folium.Map(location=[latitude, longitude], 
 					zoom_start=10)

	marker_cluster = MarkerCluster().add_to(chicago_map)

	locationslist = df.rdd.map(lambda x: ((x.Description, x.prediction), (x.Latitude, x.Longitude)))
	locationlist = locationslist.collect()
	for index in range(0, len(locationlist)):
		folium.Marker([locationlist[index][1][0], locationlist[index][1][1]], popup=locationlist[index][0][0]).add_to(marker_cluster)
	chicago_map.save('total_crimes.html')


