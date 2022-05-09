from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType

# 1 - Application Name - # of executors - # of cores specified here
spark = SparkSession.builder.appName("Query19_DFAPI").config("spark.executor.instances","4").config("spark.executor.cores","4").master("yarn").getOrCreate()


spark.conf.set("spark.sql.join.preferSortMergeJoin", False)

# 2 - File Location

#spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

fileSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/lineitem.csv").schema
lineitem = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(fileSchema).option("delimiter", "|").csv("/user/data/lineitem.tbl")

partSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/part.csv").schema
part = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(partSchema).option("delimiter", "|").csv("/user/data/part.tbl")


lineitem.registerTempTable("lineitem")
part.registerTempTable("part")

sqlString="select \
        sum(l_extendedprice* (1 - l_discount)) as revenue \
from \
        lineitem, \
        part \
where \
        ( \
                p_partkey = l_partkey \
                and p_brand = 'Brand#12' \
                and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') \
                and l_quantity >= 1 and l_quantity <= 1 + 10 \
                and p_size between 1 and 5 \
                and l_shipmode in ('AIR', 'AIR REG') \
                and l_shipinstruct = 'DELIVER IN PERSON' \
        ) \
        or \
        ( \
                p_partkey = l_partkey \
                and p_brand = 'Brand#23' \
                and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') \
                and l_quantity >= 10 and l_quantity <= 10 + 10 \
                and p_size between 1 and 10 \
                and l_shipmode in ('AIR', 'AIR REG') \
                and l_shipinstruct = 'DELIVER IN PERSON' \
        ) \
        or \
        ( \
                p_partkey = l_partkey \
                and p_brand = 'Brand#34' \
                and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') \
                and l_quantity >= 20 and l_quantity <= 20 + 10 \
                and p_size between 1 and 15 \
                and l_shipmode in ('AIR', 'AIR REG') \
                and l_shipinstruct = 'DELIVER IN PERSON' \
        ); \
"

tmp1 = lineitem.rdd.getNumPartitions()
tmp2 = part.rdd.getNumPartitions()

res = spark.sql(sqlString)
res.show()
spark.sql(sqlString).explain()
print("Partitions : Lineitem Part",tmp1,tmp2)





