from pyspark import SparkContext
from pyspark import SQLContext
from pyspark.sql.types import StructField, StringType, FloatType, StructType, IntegerType
from pyspark.sql import Row


sc = SparkContext(appName='SparkSQL')

sparksql = SQLContext(sc)

peopleRDD = sc.textFile("C:/Users/Admin/PycharmProjects/Spark/people.txt").map(lambda line: line.split(", "))

# row = Row("val")
# df = peopleRDD.map(row).toDF()
# df.show()
# print(df)

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", StringType(), True)])

df = sparksql.createDataFrame(peopleRDD, schema)
df.show()

# df.filter(df.age > 19).show()


df.printSchema()

df.createOrReplaceTempView("People")
sparksql.sql("SELECT SUM(age) AS ageTotal FROM People").show()

result = sparksql.sql("SELECT SUM(age) AS ageTotal FROM People")



