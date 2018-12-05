from pyspark import SparkContext
from pyspark import SQLContext
from pyspark.sql.types import StructField, StringType, FloatType, StructType, IntegerType, DataType, TimestampType
from pyspark.sql import Row
import datetime
import time
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import udf

sc = SparkContext(appName='SparkSQL')

sparksql = SQLContext(sc)


def parse(line):
    items = line.split("	")
    # time_start = datetime.datetime.strptime(items[9], "%H:%M:%S")
    # time_end = datetime.datetime.strptime(items[10], "%H:%M:%S")
    # print(time_end_timeStamp)
    # print((time_end-time_start).seconds)
    return (
        items[0], items[1], items[2], items[3], items[4], items[5], items[6], items[7], items[8], items[9], items[10],
        items[11], items[12], items[13], 0)


callRDD = sc.textFile("C:/Users/Admin/PycharmProjects/Spark/call.txt").map(parse)

schema = StructType([
    StructField("day_id", StringType(), True),
    StructField("calling_nbr", StringType(), True),
    StructField("called_nbr", StringType(), True),
    StructField("calling_optr", StringType(), True),
    StructField("called_optr", StringType(), True),
    StructField("calling_city", StringType(), True),
    StructField("called_city", StringType(), True),
    StructField("calling_roam_city", StringType(), True),
    StructField("called_roam_city", StringType(), True),
    StructField("start_time", StringType(), True),
    StructField("end_time", StringType(), True),
    StructField("raw_dur", StringType(), True),
    StructField("call_type", StringType(), True),
    StructField("calling_cell", StringType(), True),
    StructField("period", IntegerType(), True)
])

callDf = sparksql.createDataFrame(callRDD, schema)


# callDf.show()


# define timedelta function (obtain duration in seconds)
# 计算不同时间段下的通话时长
def time_delta(end, start, period):
    period_start = datetime.datetime.strptime(str(period) + ':00:00', '%H:%M:%S')
    if period == 21:
        period_end = datetime.datetime.strptime('00:00:00', '%H:%M:%S') + datetime.timedelta(days=1)
    else:
        period_end = datetime.datetime.strptime(str(period + 3) + ':00:00', '%H:%M:%S')
    end_time = datetime.datetime.strptime(end, '%H:%M:%S')
    start_time = datetime.datetime.strptime(start, '%H:%M:%S')
    if end_time < start_time:
        end_time += datetime.timedelta(days=1)
        if period == 0:
            period_start += datetime.timedelta(days=1)
            period_end += datetime.timedelta(days=1)
    if (period_start <= start_time < period_end) or (period_start <= end_time < period_end):
        if start_time >= period_start and period_end >= end_time:
            delta = (end_time - start_time).seconds
            return delta
        elif start_time >= period_start and period_end < end_time:
            delta = (period_end - start_time).seconds
            return delta
        elif start_time < period_start and period_end >= end_time:
            delta = (end_time - period_start).seconds
            return delta
    elif start_time < period_start and end_time > period_end:
        delta = 3 * 60 * 60
        return delta
    else:
        delta = 0
        return delta


# register as a UDF
get_duration = udf(time_delta, IntegerType())

for i in range(0, 8):
    callDf = callDf.withColumn('Duration' + str(i), get_duration(callDf.end_time, callDf.start_time, callDf.period))
    callDf = callDf.withColumn('period', callDf.period + 3)
callDf.show()

callDf.createOrReplaceTempView("Call")

# callDf.toPandas().to_csv('time.csv', index=False)


# convert string to timestamp
# method 1
# callDf1 = callDf.withColumn("start_time_new", callDf['start_time'].cast(TimestampType()))

# convert string to timestamp
# method 2
# callDf_time = callDf.select('*', to_timestamp('start_time', 'HH:mm:ss').cast(TimestampType()).alias('new_start_time'))
# callDf_time = callDf_time.select('*', to_timestamp('end_time', 'HH:mm:ss').cast(TimestampType()).alias('new_end_time'))
# callDf_time.show()


# 每日平均通话次数
# result = sparksql.sql("SELECT calling_nbr, COUNT(Call.calling_nbr) AS num FROM Call GROUP BY calling_nbr")
# result.show()
# result.toPandas().to_csv('result.csv', index=False)


# 不同通话类型下各个运营商的数量
# result = sparksql.sql(
#     "SELECT calling_optr, COUNT(Call.calling_nbr) AS num FROM Call WHERE calling_city = called_city GROUP BY calling_optr")
# result.show()

# 计算个人通话时长所占比例
result2 = sparksql.sql(
    "SELECT calling_nbr, SUM(Call.Duration1)/SUM(raw_dur) AS proportion1, SUM(Call.Duration2)/SUM(raw_dur) AS proportion2 FROM Call GROUP BY calling_nbr")
result2.show()

# callDf.toPandas().to_csv('call.csv', index=False)
