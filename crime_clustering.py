from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.feature import VectorAssembler


def create_clusters(SQLContext):
	specific_data = sqlContext.sql('SELECT Description, Date, Latitude, Longitude FROM crime_data')

	#create different k-means outputs depending on the date since that is what we are testing
	

	vecAssembler = VectorAssembler(inputCols=['latitude', 'longitude'], outputCol="features")
    df_kmeans = vecAssembler.transform(specific_data).select('Description', 'features', 'latitude', 'longitude')
    df_kmeans.show()

    # cost = [0] * 40
    # for k in range(2,40):
    #     kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
    #     model = kmeans.fit(df_kmeans.sample(False,0.1, seed=42))
    #     cost[k] = model.computeCost(df_kmeans) # requires Spark 2.0 or later
    # print (cost)

    k = 35
    kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
    model = kmeans.fit(df_kmeans)
    centers = model.clusterCenters()

    print("Cluster Centers: ")
    for center in centers:
        print(center)

    transformed = model.transform(df_kmeans).select('Description', 'prediction', 'latitude', 'longitude')
    rows = transformed.collect()
    df_pred = sqlContext.createDataFrame(rows)
    df_pred.show()

    return_rdd = df_pred.rdd.map(lambda x: (x.prediction, (Description, (x.latitude, x.longitude))))

    print (return_rdd.collect())