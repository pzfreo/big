import findspark
findspark.init()

import time
from dateutil import parser

from pyspark.sql import Row, SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.csv('/home/big/sql/*.csv', header=True)

df.show(1)

convertTime = lambda t: parser.parse(t.replace('?',' '))

clean = lambda s: Row(stationid=s.Station_ID, 
    time=convertTime(s.Interval_End_Time),
    direction=s.Wind_Direction_Deg, 
    temp=s.Ambient_Temperature_Deg_C, 
    velocity=s.Wind_Velocity_Mtr_Sec)

newDF = df.rdd.map(clean).toDF()

newDF.show(1)

newDF.write.format("org.apache.spark.sql.cassandra").mode('append').options(table="winddata", keyspace="wind").save()

print 'done'