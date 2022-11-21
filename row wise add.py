from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__=='__main__':
    spark = SparkSession.builder.master("local[*]").appName("row wise").getOrCreate()

    data =[(1,'ram','MH'),(2,'shyam','GH'),(3,'ankita','MH'),(4,'Anita','GH')]
    header = ['id','Name','State']

    #data=[Row(id=1,Name='ram',address=Row(city='pune',state='MH')),Row(id=2,Name='sham',address=
    #                Row(city='mumbai',state='MH'))]
    in_rdd = spark.sparkContext.parallelize(data)

    #create data farme
    df = in_rdd.toDF(header)
    df.show()

    #df1 = spark.createDataFrame(data)
    #df1 = spark.createDataFrame(in_rdd)
    #df1.show()

    #selecting data from table
    #df.select('id').show()
    #df.select('name').show()
    #df.select('address.city').show()
    #df.select('address').show()

    #df.select(col('address.city')).show()

    #df.select(col('address.*')).show()

    #df.select(col('name')).show()

    #change coloumn name
    #df1 = df.withColumn("id",col("id").cast(DoubleType()))
    #df1.show()
    #df.printSchema()

    #rename coloumn name
    #df.withColumn("Name","first_name").printSchema()

    #delete coloumn
    #df.drop("id").printSchema()

    #filter
    #df.filter(df.Name =='ram').show()
    #df.filter(df.id == 2).show()
    #df.filter(df.State == 'MH').select('Name').show()
    #df.filter(df.id > 2).show()

    #aggregate fuunction

    df.select(count('Name')).show()
    df.select(sum('id')).show()