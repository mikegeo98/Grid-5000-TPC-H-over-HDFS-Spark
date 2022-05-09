from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType

# 1 - Application Name - # of executors - # of cores specified here
spark = SparkSession.builder.appName("Query13_DFAPI").config("spark.executor.instances","4").config("spark.executor.cores","4").master("yarn").getOrCreate()


spark.conf.set("spark.sql.join.preferSortMergeJoin", False)

# 2 - File Location

#spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

customerSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/customer.csv").schema
customer = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(customerSchema).option("delimiter", "|").csv("/user/data/customer.tbl")

orderSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/orders.csv").schema
orders = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(orderSchema).option("delimiter", "|").csv("/user/data/orders.tbl")


orders.registerTempTable("orders")
customer.registerTempTable("customer")


sqlString= "select \
        c_count, \
        count(*) as custdist \
from \
        ( \
                select \
                        c_custkey, \
                        count(o_orderkey) \
                from \
                        customer left outer join orders on \
                                c_custkey = o_custkey \
                                and o_comment not like '%special%requests%' \
                group by \
                        c_custkey \
        ) as c_orders (c_custkey, c_count) \
group by \
        c_count \
order by \
        custdist desc, \
        c_count desc;"

tmp1 = customer.rdd.getNumPartitions()
tmp2 = orders.rdd.getNumPartitions()

res = spark.sql(sqlString)
res.show()
spark.sql(sqlString).explain()
print("Partitions : Customer  Orders ",tmp1,tmp2)

