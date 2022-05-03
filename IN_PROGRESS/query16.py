from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType

spark = SparkSession.builder.appName("Query1_DFAPI").getOrCreate()

partSchema = spark.read.options(header="true", inferSchema = "true").csv("/home/mikeg/Documents/databases/TPC-H/TPC-H_Tools_v3.0.0/dbgen/tmpqueries/schema/part.csv").schema
part = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(partSchema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/part.tbl")

partsuppSchema = spark.read.options(header="true", inferSchema = "true").csv("/home/mikeg/Documents/databases/TPC-H/TPC-H_Tools_v3.0.0/dbgen/tmpqueries/schema/partsupp.csv").schema
partsupp = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(partsuppSchema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/partsupp.tbl")

supplierSchema = spark.read.options(header="true", inferSchema = "true").csv("/home/mikeg/Documents/databases/TPC-H/TPC-H_Tools_v3.0.0/dbgen/tmpqueries/schema/supplier.csv").schema
supplier = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(supplierSchema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/supplier.tbl")

part.registerTempTable("part")
partsupp.registerTempTable("partsupp")
supplier.registerTempTable("supplier")

sqlString= "select \
        p_brand, \
        p_type, \
        p_size, \
        count(distinct ps_suppkey) as supplier_cnt \
from \
        partsupp, \
        part \
where \
        p_partkey = ps_partkey \
        and p_brand <> 'Brand#45' \
        and p_type not like 'MEDIUM POLISHED%' \
        and p_size in (49, 14, 23, 45, 19, 3, 36, 9) \
        and ps_suppkey not in ( \
                select \
                        s_suppkey \
                from \
                        supplier \
                where \
                        s_comment like '%Customer%Complaints%' \
        ) \
group by \
        p_brand, \
        p_type, \
        p_size \
order by \
        supplier_cnt desc, \
        p_brand, \
        p_type, \
        p_size;"


res = spark.sql(sqlString)
res.show()


