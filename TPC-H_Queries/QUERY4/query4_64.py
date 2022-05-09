from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType


# 1 - Application Name - # of executors - # of cores specified here
spark = SparkSession.builder.appName("Query4_DFAPI").config("spark.executor.instances","6").config("spark.executor.cores","4").master("yarn").getOrCreate()


spark.conf.set("spark.sql.join.preferSortMergeJoin", False)

# 2 - File Location

#spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

fileSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/lineitem.csv").schema
lineitem = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(fileSchema).option("delimiter", "|").csv("/user/data/lineitem.tbl")

orderSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/orders.csv").schema
orders = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(orderSchema).option("delimiter", "|").csv("/user/data/orders.tbl")

lineitem.registerTempTable("lineitem")
orders.registerTempTable("orders")

sqlString="select \
        o_orderpriority, \
        count(*) as order_count \
from \
        orders \
where \
        o_orderdate >= date '1993-07-01' \
        and o_orderdate < date '1993-07-01' + interval '3' month \
        and exists ( \
                select \
                        * \
                from \
                        lineitem \
                where \
                        l_orderkey = o_orderkey \
                        and l_commitdate < l_receiptdate \
        ) \
group by \
        o_orderpriority \
order by \
        o_orderpriority; \
"
tmp1 = lineitem.rdd.getNumPartitions()
tmp2 = orders.rdd.getNumPartitions()
res = spark.sql(sqlString)
res.show()
spark.sql(sqlString).explain()
print("Partitions no lineitem orders",tmp1,tmp2)



