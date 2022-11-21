from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark=SparkSession.builder.master("local[*]").appName("data frame session").getOrCreate()

    data =[('ram','male',30),('kiran','male',27),('sneha','female',22)]
    header = ['Name','Gender','Age']

    #1.create dataframe from existing RDD---rows and cloumn from----without coloumn name
    in_rdd = spark.sparkContext.parallelize(data)
    #print(in_rdd.collect())
    #in_Df = in_rdd.toDF()
    #in_Df.show()

    #create dataframe with coloumn name
    #in_DF1 = in_rdd.toDF(header)
    #in_DF1.show()
    #in_DF1.printSchema()

    #2.create dataframe
    #in_DF2 = spark.createDataFrame(data , schema=header)
    #in_DF2.show()
    #in_DF2.printSchema()

    #3.create dataframe using input file
    in_DF3 = spark.read.text(r"C:\Users\Admin\PycharmProjects\myproject\student.txt")
    in_DF3.show()
    #in_DF3.printSchema()


