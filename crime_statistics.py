from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import argparse

pops = {1:54991,2:71942,
3:56362,
4:39493,
5:31867,
6:94368,
7:64116,
8:80484,
9:11187,
10:37023,
11:25448,
12:18508,
13:17931,
14:51542,
15:64124,
16:53359,
17:41932,
18:13426,
19:78743,
20:25010,
21:39262,
22:73595,
23:56323,
24:81432,
25:98514,
26:18001,
27:20567,
28:54881,
29:35912,
30:79288,
31:35769,
32:29283,
33:21390,
34:13391,
35:18238,
36:5918,
37:2876,
38:2192,
39:17841,
40:11717,
41:25681,
42:25983,
43:49767,
44:31028,
45:10185,
46:31198,
47:2916,
48:13812,
49:44619,
50:7325,
51:15109,
52:23042,
53:29651,
54:6482,
55:9426,
56:34513,
57:13393,
58:45368,
59:15612,
60:31977,
61:44377,
62:18109,
63:39894,
64:23139,
65:33355,
66:55628,
67:35505,
68:30654,
69:32602,
70:41081,
71:48743,
72:20034,
73:26493,
74:19093,
75:22544,
76:12756,
77:56521}

def primaryCounts(sqlContext):
    '''
    Args:
        sc: The Spark Context
        sqlContext: The Spark SQL context
        reviews_filename: Filename of the Amazon reviews file to use, where each line represents a review
    Returns:
    	most frequent crimes/where violent crimes occur        
    '''
   
    ratings = sqlContext.sql("SELECT COUNT(`Primary Type`), `Primary Type` FROM stats_data GROUP BY `Primary Type` ORDER BY COUNT(`Primary Type`) DESC")    	
    
    # for i in ratings.collect():
    # 	print('*'*200 + str(i) + '*'*200)   
    

    return ratings.collect()

def communityAreaCounts(sqlContext):
    '''
    Args:
        sc: The Spark Context
        sqlContext: The Spark SQL context
        reviews_filename: Filename of the Amazon reviews file to use, where each line represents a review
    Returns:
    	most frequent crimes/where violent crimes occur        
    '''
   
    ratings = sqlContext.sql("SELECT COUNT(`Community Area`) , `Community Area` FROM stats_data GROUP BY `Community Area` ORDER BY `Community Area` ASC")    	
    corrected = []
    collect = ratings.collect()
    for i in range(1,77):
    	first = str(i)
    	second = collect[1]/pops[i]
    	corrected.append((first, second))
    	print('*'*200 + str(corrected[-1]) + '*'*200)   
    return corrected
    # for i in ratings.collect():   
    # return ratings.collect()

def descriptionCounts(sqlContext):
    '''
    Args:
        sc: The Spark Context
        sqlContext: The Spark SQL context
        reviews_filename: Filename of the Amazon reviews file to use, where each line represents a review
    Returns:
    	most frequent crimes/where violent crimes occur        
    '''
   
    ratings = sqlContext.sql("SELECT COUNT(Description), Description FROM stats_data GROUP BY Description ORDER BY COUNT(Description) DESC")   	
    
    # for i in ratings.collect():
    # 	print('*'*200 + str(i) + '*'*200)   
    

    return ratings.collect()

def descriptionsForPrimaries(sqlContext):
    '''
    Args:
        sc: The Spark Context
        sqlContext: The Spark SQL context
        reviews_filename: Filename of the Amazon reviews file to use, where each line represents a review
    Returns:
    	most frequent crimes/where violent crimes occur        
    '''
    top4 = ['THEFT','BATTERY','CRIMINAL DAMAGE','NARCOTICS']
    d = {}
    for primary in top4:
    	ratings = sqlContext.sql("SELECT COUNT(Description), Description FROM stats_data WHERE `Primary Type` = '" + primary + "' GROUP BY Description ORDER BY COUNT(Description) DESC")   	
    	a = []
    	for i in range(0,5):
    		a.append(ratings.collect()[i])
    	d[primary] = a
    

    return d

def descriptionsForCommunities(sqlContext):
    '''
    Args:
        sc: The Spark Context
        sqlContext: The Spark SQL context
        reviews_filename: Filename of the Amazon reviews file to use, where each line represents a review
    Returns:
    	most frequent crimes/where violent crimes occur        
    '''
    d = {}
    top6 = [25,8,43,23,24,67, 9]
    for com in top6:
    	ratings = sqlContext.sql("SELECT COUNT(Description), Description FROM stats_data WHERE `Community Area` = '" + str(com) + "' GROUP BY Description ORDER BY COUNT(Description) DESC")   	
    	a = []
    	for i in range(0,5):
    		a.append(ratings.collect()[i])
    	d[com] = a
    

    return d

def communitiesByYear(sqlContext):
    '''
    Args:
        sc: The Spark Context
        sqlContext: The Spark SQL context
        reviews_filename: Filename of the Amazon reviews file to use, where each line represents a review
    Returns:
    	most frequent crimes/where violent crimes occur        
    '''
    d = {}
    for year in range(2001,2019):
    	ratings = sqlContext.sql("SELECT COUNT(`Community Area`), `Community Area` FROM stats_data WHERE `Date` LIKE '%" + str(year) + "%' GROUP BY `Community Area` ORDER BY COUNT(`Community Area`) DESC")   	
    	a = []	
    	if len(ratings.collect()) >= 10:
    		for i in range(0,10):
    			a.append(ratings.collect()[i])
    	d[year] = a    

    return d

def primariesByYear(sqlContext):
    '''
    Args:
        sc: The Spark Context
        sqlContext: The Spark SQL context
        reviews_filename: Filename of the Amazon reviews file to use, where each line represents a review
    Returns:
    	most frequent crimes/where violent crimes occur        
    '''
    d = {}
    for year in range(2001,2019):
    	ratings = sqlContext.sql("SELECT COUNT(`Primary Type`), `Primary Type` FROM stats_data WHERE `Date` LIKE '%" + str(year) + "%' GROUP BY `Primary Type` ORDER BY COUNT(`Primary Type`) DESC")   	
    	a = []
    	if len(ratings.collect()) >= 10:
    		for i in range(0,10):
    			a.append(ratings.collect()[i])
    	d[year] = a    

    return d