from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType

# 1 - Application Name - # of executors - # of cores specified here
spark = SparkSession.builder.appName("Query16_DFAPI").config("spark.executor.instances","4").config("spark.executor.cores","4").master("yarn").getOrCreate()


spark.conf.set("spark.sql.join.preferSortMergeJoin", False)

# 2 - File Location

#spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

partSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/part.csv").schema
part = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(partSchema).option("delimiter", "|").csv("/user/data/part.tbl")

supplierSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/supplier.csv").schema
supplier = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(supplierSchema).option("delimiter", "|").csv("/user/data/supplier.tbl")

partsuppSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/partsupp.csv").schema
partsupp = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(partsuppSchema).option("delimiter", "|").csv("/user/data/partsupp.tbl")

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

tmp5 = part.rdd.getNumPartitions()
tmp6 = supplier.rdd.getNumPartitions()
tmp7 = partsupp.rdd.getNumPartitions()
res = spark.sql(sqlString)
res.show()
spark.sql(sqlString).explain()
print("Partitions : Part Supplier Partsupp",tmp5,tmp6,tmp7)



