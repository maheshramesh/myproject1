from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("data").getOrCreate()

    #d = ["bwt is class,bwt is very good class"]
    #file_rdd = spark.sparkContext.parallelize(d)
    #print(file_rdd.collect())
    #filter_rdd = file_rdd.flatMap(lambda x: x.split(" "))
    #print(filter_rdd.collect())
    #map_rdd = filter_rdd.map(lambda x: (x, 1))
    #print(map_rdd.collect())
    #reduce_rdd = map_rdd.reduceByKey(lambda x, y: x+y)
    #print(reduce_rdd.collect())

    #data = ["bwt @ is class bwt is ! brainworks team <> team @ work # is best"]
    #file1_rdd = spark.sparkContext.parallelize(data)
    #print(file1_rdd.collect())
    #flat_rdd = file1_rdd.flatMap(lambda x: x.split(" "))
    #print(flat_rdd.collect())
    #map1_rd = flat_rdd.map(lambda x: (x,1))
    #print(map1_rd.collect())
    #fil_rdd = map1_rd.reduceByKey(lambda x,y: x+y)
    #print(fil_rdd.collect())
    #filter_rdd = fil_rdd.filter(lambda x: '@' in x)
    #print(filter_rdd.collect())

    #header = StructType([StructField("id",IntegerType()),
    #                          StructField("name",StringType()),
    #                          StructField("age",IntegerType()),
    #                          StructField("salary",DoubleType())])

    #df = spark.read.load(r"C:\Users\Admin\PycharmProjects\myproject\data.txt",format="txt",
    #                     schema=schema_data)
    #df.show()
    df1 = spark.read.csv(r"D:\Thonny\data1.csv", inferSchema=True, header=True)
    df1.show()