from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType
if __name__=='__main__':
    spark = SparkSession.builder.master("local[*]").appName("empty Dataframe").getOrCreate()
    #print(spark)

    #empty RDD
    header = ['name','gender']
    in_rdd = spark.sparkContext.emptyRDD()
    print(in_rdd.getNumPartitions())

    #custom Schema
    sc_data = StructType([StructField("id",IntegerType(),False),StructField("name",StringType())])

    #create empty dataframe from empty rdd
    df = spark.createDataFrame(in_rdd,sc_data)
    print(df.printSchema())
    df.show()
    df1=in_rdd.toDF(sc_data)
    df1.show()
    print(df1.printSchema())