from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext


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
