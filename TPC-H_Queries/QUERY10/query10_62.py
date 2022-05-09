from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType
from datetime import date,time, datetime

# 1 - Application Name - # of executors - # of cores specified here
spark = SparkSession.builder.appName("Query10_DFAPI").config("spark.executor.instances","6").config("spark.executor.cores","2").master("yarn").getOrCreate()


spark.conf.set("spark.sql.join.preferSortMergeJoin", False)

# 2 - File Location

#spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)


fileSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/lineitem.csv").schema
lineitem = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(fileSchema).option("delimiter", "|").csv("/user/data/lineitem.tbl")

orderSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/orders.csv").schema
orders = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(orderSchema).option("delimiter", "|").csv("/user/data/orders.tbl")

customerSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/customer.csv").schema
customer = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(customerSchema).option("delimiter", "|").csv("/user/data/customer.tbl")

nationSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/nation.csv").schema
nation = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(nationSchema).option("delimiter", "|").csv("/user/data/nation.tbl")

querySTime = datetime.now()

orders.registerTempTable("orders")
lineitem.registerTempTable("lineitem")
customer.registerTempTable("customer")
nation.registerTempTable("nation")
queryFTime = datetime.now()


sqlString="select \
        c_custkey, \
        c_name, \
        sum(l_extendedprice * (1 - l_discount)) as revenue, \
        c_acctbal, \
        n_name, \
        c_address, \
        c_phone, \
        c_comment \
from \
        customer, \
        orders, \
        lineitem, \
        nation \
where \
        c_custkey = o_custkey \
        and l_orderkey = o_orderkey \
        and o_orderdate >= date '1993-10-01' \
        and o_orderdate < date '1993-10-01' + interval '3' month \
        and l_returnflag = 'R' \
        and c_nationkey = n_nationkey \
group by \
        c_custkey, \
        c_name, \
        c_acctbal, \
        c_phone, \
        n_name, \
        c_address, \
        c_comment \
order by \
        revenue desc; \
"

tmp = customer.rdd.getNumPartitions()
tmp2 = orders.rdd.getNumPartitions()
tmp3 = lineitem.rdd.getNumPartitions()

res = spark.sql(sqlString)
res.show()
spark.sql(sqlString).explain()

print("Partitions no customer orders lineitem",tmp,tmp2,tmp3)

