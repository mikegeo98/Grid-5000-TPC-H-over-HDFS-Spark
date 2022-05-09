from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType


# 1 - Application Name - # of executors - # of cores specified here
spark = SparkSession.builder.appName("Query20_DFAPI").config("spark.executor.instances","4").config("spark.executor.cores","4").master("yarn").getOrCreate()


spark.conf.set("spark.sql.join.preferSortMergeJoin", False)

# 2 - File Location

#spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

fileSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/lineitem.csv").schema
lineitem = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(fileSchema).option("delimiter", "|").csv("/user/data/lineitem.tbl")

nationSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/nation.csv").schema
nation = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(nationSchema).option("delimiter", "|").csv("/user/data/nation.tbl")

partSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/part.csv").schema
part = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(partSchema).option("delimiter", "|").csv("/user/data/part.tbl")

supplierSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/supplier.csv").schema
supplier = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(supplierSchema).option("delimiter", "|").csv("/user/data/supplier.tbl")

partsuppSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/partsupp.csv").schema
partsupp = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(partsuppSchema).option("delimiter", "|").csv("/user/data/partsupp.tbl")

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



tmp1 = lineitem.rdd.getNumPartitions()
tmp3 = supplier.rdd.getNumPartitions()
tmp4 = part.rdd.getNumPartitions()
tmp5 = nation.rdd.getNumPartitions()
tmp7 = partsupp.rdd.getNumPartitions()
res = spark.sql(sqlString)
res.show()
spark.sql(sqlString).explain()
print("Partitions : Lineitem Supplier Part Nation Partsupp",tmp1,tmp3,tmp4,tmp5,tmp7)

