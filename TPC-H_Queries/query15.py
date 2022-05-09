from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType

# 1 - Application Name - # of executors - # of cores specified here
spark = SparkSession.builder.appName("Query15_DFAPI").config("spark.executor.instances","4").config("spark.executor.cores","4").master("yarn").getOrCreate()


spark.conf.set("spark.sql.join.preferSortMergeJoin", False)

# 2 - File Location

#spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

fileSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/lineitem.csv").schema
lineitem = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(fileSchema).option("delimiter", "|").csv("/user/data/lineitem.tbl")

supplierSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/supplier.csv").schema
supplier = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(supplierSchema).option("delimiter", "|").csv("/user/data/supplier.tbl")


lineitem.registerTempTable("lineitem")
supplier.registerTempTable("supplier")

sqlString= "create temp view revenue0 (supplier_no, total_revenue) as \
        select \
                l_suppkey, \
                sum(l_extendedprice * (1 - l_discount)) \
        from \
                lineitem \
        where \
                l_shipdate >= date '1996-01-01' \
                and l_shipdate < date '1996-01-01' + interval '3' month \
        group by \
                l_suppkey; "
sqlString2=" \
       select \
        s_suppkey, \
        s_name, \
        s_address, \
        s_phone, \
        total_revenue \
from \
        supplier, \
        revenue0 \
where \
        s_suppkey = supplier_no \
        and total_revenue = ( \
                select \
                        max(total_revenue) \
                from \
                        revenue0 \
        ) \
order by \
        s_suppkey;"
sqlString3="drop view revenue0; \
"

res = spark.sql(sqlString)
res2 = spark.sql(sqlString2)
res3 = spark.sql(sqlString3)
res2.show()

tmp1 = lineitem.rdd.getNumPartitions()
tmp2 = supplier.rdd.getNumPartitions()

spark.sql(sqlString2).explain()
print("Partitions : Lineitem Supplier",tmp1,tmp2)



