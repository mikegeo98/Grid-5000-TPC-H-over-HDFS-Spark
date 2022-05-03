from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType

spark = SparkSession.builder.appName("Query1_DFAPI").getOrCreate()

fileSchema = spark.read.options(header="true", inferSchema = "true").csv("/home/mikeg/Documents/databases/TPC-H/TPC-H_Tools_v3.0.0/dbgen/tmpqueries/schema/lineitem.csv").schema
lineitem = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(fileSchema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/lineitem.tbl")

supplierSchema = spark.read.options(header="true", inferSchema = "true").csv("/home/mikeg/Documents/databases/TPC-H/TPC-H_Tools_v3.0.0/dbgen/tmpqueries/schema/supplier.csv").schema
supplier = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(supplierSchema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/supplier.tbl")

nationSchema = spark.read.options(header="true", inferSchema = "true").csv("/home/mikeg/Documents/databases/TPC-H/TPC-H_Tools_v3.0.0/dbgen/tmpqueries/schema/nation.csv").schema
nation = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(nationSchema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/nation.tbl")

partsuppSchema = spark.read.options(header="true", inferSchema = "true").csv("/home/mikeg/Documents/databases/TPC-H/TPC-H_Tools_v3.0.0/dbgen/tmpqueries/schema/partsupp.csv").schema
partsupp = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(partsuppSchema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/partsupp.tbl")

partSchema = spark.read.options(header="true", inferSchema = "true").csv("/home/mikeg/Documents/databases/TPC-H/TPC-H_Tools_v3.0.0/dbgen/tmpqueries/schema/part.csv").schema
part = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(partSchema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/part.tbl")

supplier.registerTempTable("supplier")
nation.registerTempTable("nation")
partsupp.registerTempTable("partsupp")
part.registerTempTable("part")
lineitem.registerTempTable("lineitem")

sqlString="select \
        s_name, \
        s_address \
from \
        supplier, \
        nation \
where \
        s_suppkey in ( \
                select \
                        ps_suppkey \
                from \
                        partsupp \
                where \
                        ps_partkey in ( \
                                select \
                                        p_partkey \
                                from \
                                        part \
                                where \
                                        p_name like 'forest%' \
                        ) \
                        and ps_availqty > ( \
                                select \
                                        0.5 * sum(l_quantity) \
                                from \
                                        lineitem \
                                where \
                                        l_partkey = ps_partkey \
                                        and l_suppkey = ps_suppkey \
                                        and l_shipdate >= date '1994-01-01' \
                                        and l_shipdate < date '1994-01-01' + interval '1' year \
                        ) \
        ) \
        and s_nationkey = n_nationkey \
        and n_name = 'CANADA' \
order by \
        s_name; \
"


res = spark.sql(sqlString)
res.show()


