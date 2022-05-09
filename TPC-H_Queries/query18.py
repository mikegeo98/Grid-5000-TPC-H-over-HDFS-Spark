from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType

# 1 - Application Name - # of executors - # of cores specified here
spark = SparkSession.builder.appName("Query18_DFAPI").config("spark.executor.instances","4").config("spark.executor.cores","4").master("yarn").getOrCreate()


spark.conf.set("spark.sql.join.preferSortMergeJoin", False)

# 2 - File Location

#spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)


fileSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/lineitem.csv").schema
lineitem = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(fileSchema).option("delimiter", "|").csv("/user/data/lineitem.tbl")

orderSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/orders.csv").schema
orders = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(orderSchema).option("delimiter", "|").csv("/user/data/orders.tbl")

customerSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/customer.csv").schema
customer = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(customerSchema).option("delimiter", "|").csv("/user/data/customer.tbl")

lineitem.registerTempTable("lineitem")
orders.registerTempTable("orders")
customer.registerTempTable("customer")

sqlString="select \
        c_name, \
        c_custkey, \
        o_orderkey, \
        o_orderdate, \
        o_totalprice, \
        sum(l_quantity) \
from \
        customer, \
        orders, \
        lineitem \
where \
        o_orderkey in ( \
                select \
                        l_orderkey \
                from \
                        lineitem \
                group by \
                        l_orderkey having \
                                sum(l_quantity) > 300 \
        ) \
        and c_custkey = o_custkey \
        and o_orderkey = l_orderkey \
group by \
        c_name, \
        c_custkey, \
        o_orderkey, \
        o_orderdate, \
        o_totalprice \
order by \
        o_totalprice desc, \
        o_orderdate; \
"

tmp = customer.rdd.getNumPartitions()
tmp2 = orders.rdd.getNumPartitions()
tmp3 = lineitem.rdd.getNumPartitions()
res = spark.sql(sqlString)
res.show()
spark.sql(sqlString).explain()
print("Partitions no customer orders lineitem",tmp,tmp2,tmp3)



