from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType
from datetime import date,time,datetime

# 1 - Application Name - # of executors - # of cores specified here
spark = SparkSession.builder.appName("Query6_DFAPI").config("spark.executor.instances","6").config("spark.executor.cores","6").master("yarn").getOrCreate()


spark.conf.set("spark.sql.join.preferSortMergeJoin", False)

# 2 - File Location

#spark.conf.set("spark.sql.autoBroadcastJoinThresho


fileSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/lineitem.csv").schema
lineitem = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(fileSchema).option("delimiter", "|").csv("/user/data/lineitem.tbl")

lineitem.registerTempTable("lineitem")


sqlString="select \
        sum(l_extendedprice * l_discount) as revenue \
from \
        lineitem \
where \
        l_shipdate >= date '1994-01-01' \
        and l_shipdate < date '1994-01-01' + interval '1' year \
        and l_discount between .06 - 0.01 and .06 + 0.01 \
        and l_quantity < 24; \
"

queryStartTime = datetime.now()
res = spark.sql(sqlString)
queryStopTime = datetime.now()
runTime = queryStopTime-queryStartTime 
res.show()
spark.sql(sqlString).explain()
print("Runtime: ",runTime)

tmp = lineitem.rdd.getNumPartitions()
print("Lineitem",tmp)
