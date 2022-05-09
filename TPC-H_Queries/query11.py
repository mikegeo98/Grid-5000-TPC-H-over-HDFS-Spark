from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType

# 1 - Application Name - # of executors - # of cores specified here
spark = SparkSession.builder.appName("Query11_DFAPI").config("spark.executor.instances","4").config("spark.executor.cores","4").master("yarn").getOrCreate()


spark.conf.set("spark.sql.join.preferSortMergeJoin", False)

# 2 - File Location

#spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)


nationSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/nation.csv").schema
nation = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(nationSchema).option("delimiter", "|").csv("/user/data/nation.tbl")

supplierSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/supplier.csv").schema
supplier = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(supplierSchema).option("delimiter", "|").csv("/user/data/supplier.tbl")

partsuppSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/partsupp.csv").schema
partsupp = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(partsuppSchema).option("delimiter", "|").csv("/user/data/partsupp.tbl")

supplier.registerTempTable("supplier")
nation.registerTempTable("nation")
partsupp.registerTempTable("partsupp")


sqlString="select \
        ps_partkey, \
        sum(ps_supplycost * ps_availqty) as value \
from \
        partsupp, \
        supplier, \
        nation \
where \
        ps_suppkey = s_suppkey \
        and s_nationkey = n_nationkey \
        and n_name = 'GERMANY' \
group by \
        ps_partkey having \
                sum(ps_supplycost * ps_availqty) > ( \
                        select \
                                sum(ps_supplycost * ps_availqty) * 0.0001000000 \
                        from \
                                partsupp, \
                                supplier, \
                                nation \
                        where \
                                ps_suppkey = s_suppkey \
                                and s_nationkey = n_nationkey \
                                and n_name = 'GERMANY' \
                ) \
order by \
        value desc; \
"

tmp5 = nation.rdd.getNumPartitions()
tmp6 = supplier.rdd.getNumPartitions()
tmp7 = partsupp.rdd.getNumPartitions()
res = spark.sql(sqlString)
res.show()
spark.sql(sqlString).explain()
print("Partitions : Nation Supplier Partsupp",tmp5,tmp6,tmp7)

