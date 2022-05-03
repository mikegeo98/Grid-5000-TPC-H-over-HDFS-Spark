from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType

spark = SparkSession.builder.appName("Query1_DFAPI").getOrCreate()

fileSchema = spark.read.options(header="true", inferSchema = "true").csv("/home/mikeg/Documents/databases/TPC-H/TPC-H_Tools_v3.0.0/dbgen/tmpqueries/schema/lineitem.csv").schema
lineitem = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(fileSchema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/lineitem.tbl")

orderSchema = spark.read.options(header="true", inferSchema = "true").csv("/home/mikeg/Documents/databases/TPC-H/TPC-H_Tools_v3.0.0/dbgen/tmpqueries/schema/orders.csv").schema
orders = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(orderSchema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/orders.tbl")

customerSchema = spark.read.options(header="true", inferSchema = "true").csv("/home/mikeg/Documents/databases/TPC-H/TPC-H_Tools_v3.0.0/dbgen/tmpqueries/schema/customer.csv").schema
customer = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(customerSchema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/customer.tbl")

orders.registerTempTable("orders")
lineitem.registerTempTable("lineitem")
customer.registerTempTable("customer")

sqlString="select \
        c_name, \
        c_custkey, \
        o_orderkey, \
        o_orderdate, \
        o_totalprice, \
        sum(l_quantity) \
from \
        customer, \
        orders, \
        lineitem \
where \
        o_orderkey in ( \
                select \
                        l_orderkey \
                from \
                        lineitem \
                group by \
                        l_orderkey having \
                                sum(l_quantity) > 300 \
        ) \
        and c_custkey = o_custkey \
        and o_orderkey = l_orderkey \
group by \
        c_name, \
        c_custkey, \
        o_orderkey, \
        o_orderdate, \
        o_totalprice \
order by \
        o_totalprice desc, \
        o_orderdate; \
"

res = spark.sql(sqlString)
res.show()


