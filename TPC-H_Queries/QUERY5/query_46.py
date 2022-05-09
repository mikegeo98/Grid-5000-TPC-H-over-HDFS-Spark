from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType

# 1 - Application Name - # of executors - # of cores specified here
spark = SparkSession.builder.appName("Query5_DFAPI").config("spark.executor.instances","4").config("spark.executor.cores","6").master("yarn").getOrCreate()


spark.conf.set("spark.sql.join.preferSortMergeJoin", False)

# 2 - File Location

#spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

customerSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/customer.csv").schema
customer = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(customerSchema).option("delimiter", "|").csv("/user/data/customer.tbl")

fileSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/lineitem.csv").schema
lineitem = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(fileSchema).option("delimiter", "|").csv("/user/data/lineitem.tbl")

orderSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/orders.csv").schema
orders = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(orderSchema).option("delimiter", "|").csv("/user/data/orders.tbl")

supplierSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/supplier.csv").schema
supplier = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(supplierSchema).option("delimiter", "|").csv("/user/data/supplier.tbl")

nationSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/nation.csv").schema
nation = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(nationSchema).option("delimiter", "|").csv("/user/data/nation.tbl")

regionSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/region.csv").schema
region = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(regionSchema).option("delimiter", "|").csv("/user/data/region.tbl")


customer.registerTempTable("customer")
orders.registerTempTable("orders")
lineitem.registerTempTable("lineitem")
supplier.registerTempTable("supplier")
nation.registerTempTable("nation")
region.registerTempTable("region")


sqlString="select \
        n_name, \
        sum(l_extendedprice * (1 - l_discount)) as revenue \
from \
        customer, \
        orders, \
        lineitem, \
        supplier, \
        nation, \
        region \
where \
        c_custkey = o_custkey \
        and l_orderkey = o_orderkey \
        and l_suppkey = s_suppkey \
        and c_nationkey = s_nationkey \
        and s_nationkey = n_nationkey \
        and n_regionkey = r_regionkey \
        and r_name = 'ASIA' \
        and o_orderdate >= date '1994-01-01' \
        and o_orderdate < date '1994-01-01' + interval '1' year \
group by \
        n_name \
order by \
        revenue desc; \
"
tmp = customer.rdd.getNumPartitions()
tmp2 = orders.rdd.getNumPartitions()
tmp3 = lineitem.rdd.getNumPartitions()
tmp4 = supplier.rdd.getNumPartitions()
res = spark.sql(sqlString)
res.show()
spark.sql(sqlString).explain()
print("Partitions no customer orders lineitem supplier",tmp,tmp2,tmp3,tmp4)


