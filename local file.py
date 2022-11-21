from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType,StructField,DoubleType

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("local file import").getOrCreate()

    sc_data = StructType([StructField("roll_no", IntegerType()),StructField("name", StringType()),
                          StructField("class",StringType()),StructField("city",StringType())])
    #print(sc_data)

    #program not running
    in_data = spark.read.load(r"D:\Thonny\data1.csv",
                              format="csv",schema=sc_data)

    in_data.show()

    #in_rdd = spark.sparkContext.textFile(r"D:\Thonny\dept1.txt")

    #df = in_rdd.toDF(sc_data)
    #df.show()

    data = [(1,"ram",25,20000),(2,'sham',29,30000),(3,'kiran',31,10000),(4,'mina',34,14000)]

    rdd_file = spark.sparkContext.parallelize(data)

    header = ['id','name','age','salary']

    df_rd = rdd_file.toDF(header)
    df_rd.show()
    print(df_rd.printSchema())

    df_rd.select(sum('salary')).show()
    df_rd.select(max('salary')).select('name').show()

