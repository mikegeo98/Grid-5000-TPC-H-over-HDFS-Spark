from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType

spark = SparkSession.builder.appName("Query1_DFAPI").getOrCreate()

supplierSchema = spark.read.options(header="true", inferSchema = "true").csv("/home/mikeg/Documents/databases/TPC-H/TPC-H_Tools_v3.0.0/dbgen/tmpqueries/schema/supplier.csv").schema
supplier = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(supplierSchema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/supplier.tbl")

fileSchema = spark.read.options(header="true", inferSchema = "true").csv("/home/mikeg/Documents/databases/TPC-H/TPC-H_Tools_v3.0.0/dbgen/tmpqueries/schema/lineitem.csv").schema
lineitem = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(fileSchema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/lineitem.tbl")

orderSchema = spark.read.options(header="true", inferSchema = "true").csv("/home/mikeg/Documents/databases/TPC-H/TPC-H_Tools_v3.0.0/dbgen/tmpqueries/schema/orders.csv").schema
orders = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(orderSchema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/orders.tbl")

nationSchema = spark.read.options(header="true", inferSchema = "true").csv("/home/mikeg/Documents/databases/TPC-H/TPC-H_Tools_v3.0.0/dbgen/tmpqueries/schema/nation.csv").schema
nation = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(nationSchema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/nation.tbl")

supplier.registerTempTable("supplier")
orders.registerTempTable("orders")
lineitem.registerTempTable("lineitem")
nation.registerTempTable("nation")



sqlString="select \
        s_name, \
        count(*) as numwait \
from \
        supplier, \
        lineitem l1, \
        orders, \
        nation \
where \
        s_suppkey = l1.l_suppkey \
        and o_orderkey = l1.l_orderkey \
        and o_orderstatus = 'F' \
        and l1.l_receiptdate > l1.l_commitdate \
        and exists ( \
                select \
                        * \
                from \
                        lineitem l2 \
                where \
                        l2.l_orderkey = l1.l_orderkey \
                        and l2.l_suppkey <> l1.l_suppkey \
        ) \
        and not exists ( \
                select \
                        * \
                from \
                        lineitem l3 \
                where \
                        l3.l_orderkey = l1.l_orderkey \
                        and l3.l_suppkey <> l1.l_suppkey \
                        and l3.l_receiptdate > l3.l_commitdate \
        ) \
        and s_nationkey = n_nationkey \
        and n_name = 'SAUDI ARABIA' \
group by \
        s_name \
order by \
        numwait desc, \
        s_name; \
"


res = spark.sql(sqlString)
res.show()


