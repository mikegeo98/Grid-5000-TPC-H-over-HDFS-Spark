from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType

spark = SparkSession.builder.appName("Query1_DFAPI").getOrCreate()

orderSchema = spark.read.options(header="true", inferSchema = "true").csv("/home/mikeg/Documents/databases/TPC-H/TPC-H_Tools_v3.0.0/dbgen/tmpqueries/schema/orders.csv").schema
orders = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(orderSchema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/orders.tbl")

customerSchema = spark.read.options(header="true", inferSchema = "true").csv("/home/mikeg/Documents/databases/TPC-H/TPC-H_Tools_v3.0.0/dbgen/tmpqueries/schema/customer.csv").schema
customer = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(customerSchema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/customer.tbl")

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


res = spark.sql(sqlString)
res.show()


