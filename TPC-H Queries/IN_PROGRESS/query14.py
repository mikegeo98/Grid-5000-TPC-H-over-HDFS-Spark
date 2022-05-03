from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType

spark = SparkSession.builder.appName("Query1_DFAPI").getOrCreate()

fileSchema = spark.read.options(header="true", inferSchema = "true").csv("/home/mikeg/Documents/databases/TPC-H/TPC-H_Tools_v3.0.0/dbgen/tmpqueries/schema/lineitem.csv").schema
lineitem = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(fileSchema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/lineitem.tbl")


partSchema = spark.read.options(header="true", inferSchema = "true").csv("/home/mikeg/Documents/databases/TPC-H/TPC-H_Tools_v3.0.0/dbgen/tmpqueries/schema/part.csv").schema
part = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(partSchema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/part.tbl")



lineitem.registerTempTable("lineitem")
part.registerTempTable("part")


sqlString= "select \
        100.00 * sum(case \
                when p_type like 'PROMO%' \
                        then l_extendedprice * (1 - l_discount) \
                else 0 \
        end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue \
from \
        lineitem, \
        part \
where \
        l_partkey = p_partkey \
        and l_shipdate >= date '1995-09-01' \
        and l_shipdate < date '1995-09-01' + interval '1' month; \
"


res = spark.sql(sqlString)
res.show()


