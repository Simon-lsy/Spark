from pyspark import SparkContext
from pyspark import SQLContext
from pyspark.sql.types import StructField, StringType, FloatType, StructType, IntegerType, DataType, TimestampType
import datetime
from pyspark.sql.functions import udf

sc = SparkContext(appName='SparkSQL')

sparksql = SQLContext(sc)

def parse(line):
    items = line.split("	")
    return (
        items[0], items[1], items[2], items[3], items[4], items[5], items[6], items[7], items[8], items[9], items[10],
        items[11], items[12], items[13], 0)


# callRDD = sc.textFile("C:/Users/Admin/PycharmProjects/Spark/call.txt").map(parse)
callRDD = sc.textFile("hdfs://master:9000/data/tb_call_201202_random.txt").map(parse)

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

for i in range(1, 9):
    callDf = callDf.withColumn('Duration' + str(i), get_duration(callDf.end_time, callDf.start_time, callDf.period))
    callDf = callDf.withColumn('period', callDf.period + 3)
# callDf.show()

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
callNumber = sparksql.sql("SELECT calling_nbr, round(COUNT(Call.calling_nbr)/29, 2) AS num FROM Call GROUP BY calling_nbr")
callNumber.toPandas().to_csv('callNumber.csv', index=False)


# 不同通话类型下各个运营商的数量占比
# 本地
local = sparksql.sql(
    "SELECT called_optr, round(COUNT(Call.called_nbr) * 1.00/(select COUNT(Call.called_nbr) from Call) ,2) AS num FROM Call WHERE call_type == 1 GROUP BY called_optr")
local.toPandas().to_csv('local.csv', index=False)

# 长途
longDistance = sparksql.sql(
    "SELECT called_optr, round(COUNT(Call.called_nbr) * 1.00/(select COUNT(Call.called_nbr) from Call) ,2) AS num FROM Call WHERE call_type == 2 GROUP BY called_optr")
longDistance.toPandas().to_csv('longDistance.csv', index=False)

# 漫游
roaming = sparksql.sql(
    "SELECT called_optr, round(COUNT(Call.called_nbr) * 1.00/(select COUNT(Call.called_nbr) from Call) ,2) AS num FROM Call WHERE call_type == 3 GROUP BY called_optr")
roaming.toPandas().to_csv('roaming.csv', index=False)


# 计算个人通话时长所占比例
callDuration = sparksql.sql(
    "SELECT calling_nbr, SUM(Call.Duration1)/SUM(raw_dur) AS proportion1, SUM(Call.Duration2)/SUM(raw_dur) AS proportion2, SUM(Call.Duration3)/SUM(raw_dur) AS proportion3,SUM(Call.Duration4)/SUM(raw_dur) AS proportion4,SUM(Call.Duration5)/SUM(raw_dur) AS proportion5,SUM(Call.Duration6)/SUM(raw_dur) AS proportion6,SUM(Call.Duration7)/SUM(raw_dur) AS proportion7,SUM(Call.Duration8)/SUM(raw_dur) AS proportion8 FROM Call GROUP BY calling_nbr")
# callDuration.show()
callDuration.toPandas().to_csv('callDuration.csv', index=False)

