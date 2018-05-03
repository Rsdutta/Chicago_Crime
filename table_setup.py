from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
# from crime_clustering import create_clusters
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
    df = df.filter(df.Description != "SIMPLE")
    sqlContext.registerDataFrameAsTable(df, "crime_data")

def setup_table2(sc, sqlContext, reviews_filename):
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
    # df = df.filter(df.Description != "SIMPLE")
    sqlContext.registerDataFrameAsTable(df, "stats_data")


if __name__ == '__main__':

    file = "hdfs:///projects/group14/crimedata.csv"
    f = open("stats_out", "w")
    # f2 = open("stats_out2", "w")
    # f3 = open("by_year_out", "w")
    # Setup Spark
    conf = SparkConf().setAppName("crime_data_stats")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    setup_table2(sc, sqlContext, file)
    # create_clusters(sqlContext)

    primary_type_counts = primaryCounts(sqlContext)
    f.write(str(primary_type_counts))
    f.write("\n")
    # print(primary_type_counts)

    community_counts = communityAreaCounts(sqlContext)
    f.write(str(community_counts))
    # f.write("\n")    
    print(community_counts)

    description_counts = descriptionCounts(sqlContext)
    f.write(str(description_counts))
    f.write("\n")
    # print(description_counts)

    d = descriptionsForPrimaries(sqlContext)
    d2 = descriptionsForCommunities(sqlContext)
    f.write(str(d))
    f.write("\n")
    f.write(str(d2))
    # print('*'*200 + str(d))
    # print('*'*200 + str(d2))

    community_years = communitiesByYear(sqlContext)
    primary_type_years = primariesByYear(sqlContext)
    f.write(str(community_years))
    f.write("\n")
    f.write(str(primary_type_years))
    # print('*'*200 + str(community_years))
    # print('*'*200 + str(primary_type_years))




    #################################################################################################
    #                TODO:                                                                          #
    # 1. Parse the DB for crime types based on location/type of crime, time of day, etc             #
    # 2. Perform k-means clustering on the dataset using different attributes to create the clusters#
    # ###############################################################################################


