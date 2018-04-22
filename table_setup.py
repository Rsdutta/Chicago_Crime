from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from crime_clustering import create_clusters
from crime_statistics import *

##########################################################################################
# This file is the main file where we will be adding functions to see results of our     #
# code. Add functions you want to run after setup_table. Other than that add new         #
# functions to different files and import them as i did above with crime_clustering, etc #
# in order to keep this document clutter free. There are TODO's below with things we need#
# to work on.                                                                            #   
##########################################################################################

def setup_table(sc, sqlContext, reviews_filename):
    '''
    Args:
        sc: The Spark Context
        sqlContext: The Spark SQL context
        reviews_filename: Filename of the Amazon reviews file to use, where each line represents a review    
    Parse the reviews file and register it as a table in Spark SQL in this function
    '''
    df = sqlContext.read.csv(reviews_filename, header=True, inferSchema=True)
    #begin preprocessing to get rid of null values
    df = df.dropna(how='any', thresh=None, subset=None)
    df = df.drop_duplicates()
    sqlContext.registerDataFrameAsTable(df, "crime_data")



if __name__ == '__main__':

    file = "hdfs:///projects/group14/crimedata.csv"
    # Setup Spark
    conf = SparkConf().setAppName("crime_data_stats")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    setup_table(sc, sqlContext, file)
    create_clusters(sqlContext)

    #################################################################################################
    #                TODO:                                                                          #
    # 1. Parse the DB for crime types based on location/type of crime, time of day, etc             #
    # 2. Perform k-means clustering on the dataset using different attributes to create the clusters#
    # ###############################################################################################


