from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType
from datetime import date, time, datetime

#spark = SparkSession.builder.appName("Query3_DFAPI").config("spark.driver.memory","8g").config("spark.driver.memory.overhead","1g").config("yarn.nodemanager.resource.memory-mb","6144").config("spark.executor.instances", "8").config("spark.executor.pyspark.memory","1g").config("spark.yarn.am.memory","6g").master("yarn").getOrCreate()
#spark = SparkSession.builder.appName("Query3_DFAPI").config("spark.executor.instances", "6").config("spark.executor.pyspark.memory","256m").config("spark.yarn.am.memory","3g").master("yarn").getOrCreate()
#spark = SparkSession.builder.appName("Query3_DFAPI").config("spark.yarn.am.memory","10240").config("spark.executor.memory","2g").config("spark.executor.pyspark.memory","1g").config("spark.executor.instances", "1").config("spark.executor.cores","1").master("yarn").getOrCreate()
spark = SparkSession.builder.appName("Query3_DFAPI").config("spark.executor.instances","6").config("spark.executor.cores","6").master("yarn").getOrCreate()
#spark = SparkSession.builder.appName("Query3_DFAPI").getOrCreate()
spark.conf.set("spark.sql.join.preferSortMergeJoin", False)
print("We are here")
SparkConf().getAll()

fileSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/lineitem.csv").schema
lineitem = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(fileSchema).option("delimiter", "|").csv("/user/data/lineitem.tbl")

orderSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/orders.csv").schema
orders = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(orderSchema).option("delimiter", "|").csv("/user/data/orders.tbl")

customerSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/customer.csv").schema
customer = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(customerSchema).option("delimiter", "|").csv("/user/data/customer.tbl")

lineitem.registerTempTable("lineitem")
orders.registerTempTable("orders")
customer.registerTempTable("customer")

#sum(l_extendedprice * (1 - l_discount)) as revenue,
sqlString="select \
        l_orderkey, \
        sum(l_extendedprice * (1 - l_discount)) as revenue, \
        o_orderdate, \
        o_shippriority \
from \
        customer,\
        orders, \
        lineitem \
where \
        c_mktsegment = 'BUILDING' \
        and c_custkey = o_custkey \
        and l_orderkey = o_orderkey \
        and o_orderdate < date '1995-03-15' \
        and l_shipdate > date '1995-03-15' \
group by \
        l_orderkey, \
        o_orderdate, \
        o_shippriority \
order by \
        revenue desc, \
        o_orderdate; "

#lz = lineitem.rdd.glom().map(len).collect()
#l = orders.rdd.glom().map(len).collect()  # get length of each partition
#l1 = customer.rdd.glom().map(len).collect()

tmp = customer.rdd.getNumPartitions()
tmp2 = orders.rdd.getNumPartitions()
tmp3 = lineitem.rdd.getNumPartitions()
res = spark.sql(sqlString)
res.show()
spark.sql(sqlString).explain()
print("Partitions no customer orders lineitem",tmp,tmp2,tmp3)
