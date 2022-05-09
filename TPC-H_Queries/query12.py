from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType
from datetime import date, time, datetime

# 1 - Application Name - # of executors - # of cores specified here
spark = SparkSession.builder.appName("Query12_DFAPI").config("spark.executor.instances","4").config("spark.executor.cores","4").master("yarn").getOrCreate()


spark.conf.set("spark.sql.join.preferSortMergeJoin", False)

# 2 - File Location

#spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

fileSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/lineitem.csv").schema
lineitem = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(fileSchema).option("delimiter", "|").csv("/user/data/lineitem.tbl")

orderSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/orders.csv").schema
orders = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(orderSchema).option("delimiter", "|").csv("/user/data/orders.tbl")



#sqlString = "select * from region"
sqlString= "select \
        l_shipmode, \
        sum(case \
                when o_orderpriority = '1-URGENT' \
                        or o_orderpriority = '2-HIGH' \
                        then 1 \
                else 0 \
        end) as high_line_count, \
        sum(case \
                when o_orderpriority <> '1-URGENT' \
                        and o_orderpriority <> '2-HIGH' \
                        then 1 \
                else 0 \
        end) as low_line_count \
from \
        orders, \
        lineitem \
where \
        o_orderkey = l_orderkey \
        and l_shipmode in ('MAIL', 'SHIP') \
        and l_commitdate < l_receiptdate \
        and l_shipdate < l_commitdate \
        and l_receiptdate >= date '1994-01-01' \
        and l_receiptdate < date '1994-01-01' + interval '1' year \
group by \
        l_shipmode \
order by \
        l_shipmode; \
"
tmp1 = lineitem.rdd.getNumPartitions()
tmp2 = orders.rdd.getNumPartitions()

res = spark.sql(sqlString)
res.show()
spark.sql(sqlString).explain()
print("Partitions : Lineitem Orders ",tmp1,tmp2)
