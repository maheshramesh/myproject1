from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
import pyspark
if __name__ == '__main__':
    spark = SparkSession.builder \
            .master("local[*]")  \
            .appName("sparkrdd")  \
            .config("spark.driver.bindAddress", "localhost")  \
            .config("spark.vi.port","4040")  \
            .getOrCreate()

    print(spark)

    df1 = spark.read.csv(r"C:\Users\Admin\PycharmProjects\myproject\new\new.csv",
                         inferSchema=True,header=True)
    df1.show()

    windose = Window.partitionBy("id").orderBy("city")


    #df1.withColumn("Row_number",row_number().over(windose))\
    #   .select("*").show()

   # df1.withColumn("lead_val",lead("year",1).over(windose)).show()


    df1.createTempView("student")

    #spark.sql("select count(id),id from student group by id").show()
    spark.sql("select name,city,year,id,lead(year),rank() over(partition by id group by id) from student").show()





    #input("enter code to stop")
    #spark.stop()

