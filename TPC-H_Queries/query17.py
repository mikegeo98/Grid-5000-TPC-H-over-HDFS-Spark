from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType

# 1 - Application Name - # of executors - # of cores specified here
spark = SparkSession.builder.appName("Query17_DFAPI").config("spark.executor.instances","4").config("spark.executor.cores","4").master("yarn").getOrCreate()


spark.conf.set("spark.sql.join.preferSortMergeJoin", False)

# 2 - File Location

#spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

fileSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/lineitem.csv").schema
lineitem = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(fileSchema).option("delimiter", "|").csv("/user/data/lineitem.tbl")

partSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/part.csv").schema
part = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(partSchema).option("delimiter", "|").csv("/user/data/part.tbl")


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



tmp1 = lineitem.rdd.getNumPartitions()
tmp2 = part.rdd.getNumPartitions()

res = spark.sql(sqlString)
res.show()
spark.sql(sqlString).explain()
print("Partitions : Lineitem Part",tmp1,tmp2)


