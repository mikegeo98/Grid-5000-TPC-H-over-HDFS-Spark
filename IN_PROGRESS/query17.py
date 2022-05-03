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
        sum(l_extendedprice) / 7.0 as avg_yearly \
from \
        lineitem, \
        part \
where \
        p_partkey = l_partkey \
        and p_brand = 'Brand#23' \
        and p_container = 'MED BOX' \
        and l_quantity < ( \
                select \
                        0.2 * avg(l_quantity) \
                from \
                        lineitem \
                where \
                        l_partkey = p_partkey \
        );"


res = spark.sql(sqlString)
res.show()


