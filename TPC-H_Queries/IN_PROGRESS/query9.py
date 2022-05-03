from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType

spark = SparkSession.builder.appName("Query1_DFAPI").getOrCreate()

fileSchema = spark.read.options(header="true", inferSchema = "true").csv("/home/mikeg/Documents/databases/TPC-H/TPC-H_Tools_v3.0.0/dbgen/tmpqueries/schema/lineitem.csv").schema
lineitem = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(fileSchema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/lineitem.tbl")

orderSchema = spark.read.options(header="true", inferSchema = "true").csv("/home/mikeg/Documents/databases/TPC-H/TPC-H_Tools_v3.0.0/dbgen/tmpqueries/schema/orders.csv").schema
orders = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(orderSchema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/orders.tbl")

supplierSchema = spark.read.options(header="true", inferSchema = "true").csv("/home/mikeg/Documents/databases/TPC-H/TPC-H_Tools_v3.0.0/dbgen/tmpqueries/schema/supplier.csv").schema
supplier = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(supplierSchema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/supplier.tbl")

nationSchema = spark.read.options(header="true", inferSchema = "true").csv("/home/mikeg/Documents/databases/TPC-H/TPC-H_Tools_v3.0.0/dbgen/tmpqueries/schema/nation.csv").schema
nation = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(nationSchema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/nation.tbl")

regionSchema = spark.read.options(header="true", inferSchema = "true").csv("/home/mikeg/Documents/databases/TPC-H/TPC-H_Tools_v3.0.0/dbgen/tmpqueries/schema/region.csv").schema
region = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(regionSchema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/region.tbl")


partSchema = spark.read.options(header="true", inferSchema = "true").csv("/home/mikeg/Documents/databases/TPC-H/TPC-H_Tools_v3.0.0/dbgen/tmpqueries/schema/part.csv").schema
part = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(partSchema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/part.tbl")

partsuppSchema = spark.read.options(header="true", inferSchema = "true").csv("/home/mikeg/Documents/databases/TPC-H/TPC-H_Tools_v3.0.0/dbgen/tmpqueries/schema/partsupp.csv").schema
partsupp = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(partsuppSchema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/partsupp.tbl")

orders.registerTempTable("orders")
lineitem.registerTempTable("lineitem")
supplier.registerTempTable("supplier")
nation.registerTempTable("nation")
region.registerTempTable("region")
part.registerTempTable("part")
partsupp.registerTempTable("partsupp")


sqlString="select \
        nation, \
        o_year, \
        sum(amount) as sum_profit \
from \
        ( \
                select \
                        n_name as nation, \
                        extract(year from o_orderdate) as o_year, \
                        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount \
                from \
                        part, \
                        supplier, \
                        lineitem, \
                        partsupp, \
                        orders, \
                        nation \
                where \
                        s_suppkey = l_suppkey \
                        and ps_suppkey = l_suppkey \
                        and ps_partkey = l_partkey \
                        and p_partkey = l_partkey \
                        and o_orderkey = l_orderkey \
                        and s_nationkey = n_nationkey \
                        and p_name like '%green%' \
        ) as profit \
group by \
        nation, \
        o_year \
order by \
        nation, \
        o_year desc; \
"


res = spark.sql(sqlString)
res.show()


