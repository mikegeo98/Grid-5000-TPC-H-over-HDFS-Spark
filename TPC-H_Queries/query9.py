from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType

# 1 - Application Name - # of executors - # of cores specified here
spark = SparkSession.builder.appName("Query9_DFAPI").config("spark.executor.instances","4").config("spark.executor.cores","4").master("yarn").getOrCreate()


spark.conf.set("spark.sql.join.preferSortMergeJoin", False)

# 2 - File Location

#spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

fileSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/lineitem.csv").schema
lineitem = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(fileSchema).option("delimiter", "|").csv("/user/data/lineitem.tbl")

orderSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/orders.csv").schema
orders = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(orderSchema).option("delimiter", "|").csv("/user/data/orders.tbl")

nationSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/nation.csv").schema
nation = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(nationSchema).option("delimiter", "|").csv("/user/data/nation.tbl")

regionSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/region.csv").schema
region = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(regionSchema).option("delimiter", "|").csv("/user/data/region.tbl")

partSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/part.csv").schema
part = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(partSchema).option("delimiter", "|").csv("/user/data/part.tbl")

supplierSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/supplier.csv").schema
supplier = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(supplierSchema).option("delimiter", "|").csv("/user/data/supplier.tbl")

partsuppSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/partsupp.csv").schema
partsupp = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(partsuppSchema).option("delimiter", "|").csv("/user/data/partsupp.tbl")

orders.registerTempTable("orders")
lineitem.registerTempTable("lineitem")
supplier.registerTempTable("supplier")
nation.registerTempTable("nation")
region.registerTempTable("region")
part.registerTempTable("part")
partsupp.registerTempTable("partsupp")


sqlString="select \
        nation, \
        o_year, \
        sum(amount) as sum_profit \
from \
        ( \
                select \
                        n_name as nation, \
                        extract(year from o_orderdate) as o_year, \
                        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount \
                from \
                        part, \
                        supplier, \
                        lineitem, \
                        partsupp, \
                        orders, \
                        nation \
                where \
                        s_suppkey = l_suppkey \
                        and ps_suppkey = l_suppkey \
                        and ps_partkey = l_partkey \
                        and p_partkey = l_partkey \
                        and o_orderkey = l_orderkey \
                        and s_nationkey = n_nationkey \
                        and p_name like '%green%' \
        ) as profit \
group by \
        nation, \
        o_year \
order by \
        nation, \
        o_year desc; \
"


tmp1 = lineitem.rdd.getNumPartitions()
tmp2 = orders.rdd.getNumPartitions()
tmp3 = supplier.rdd.getNumPartitions()
tmp4 = part.rdd.getNumPartitions()
tmp5 = nation.rdd.getNumPartitions()
tmp6 = region.rdd.getNumPartitions()
tmp7 = partsupp.rdd.getNumPartitions()
res = spark.sql(sqlString)
res.show()
spark.sql(sqlString).explain()
print("Partitions : Lineitem Orders Supplier Part Nation Region Partsupp",tmp1,tmp2,tmp3,tmp4,tmp5,tmp6,tmp7)
