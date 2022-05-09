from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType

# 1 - Application Name - # of executors - # of cores specified here
spark = SparkSession.builder.appName("Query22_DFAPI").config("spark.executor.instances","4").config("spark.executor.cores","4").master("yarn").getOrCreate()


spark.conf.set("spark.sql.join.preferSortMergeJoin", False)

# 2 - File Location

#spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

customerSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/customer.csv").schema
customer = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(customerSchema).option("delimiter", "|").csv("/user/data/customer.tbl")

orderSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/orders.csv").schema
orders = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(orderSchema).option("delimiter", "|").csv("/user/data/orders.tbl")


orders.registerTempTable("orders")
customer.registerTempTable("customer")

sqlString="select \
        cntrycode, \
        count(*) as numcust, \
        sum(c_acctbal) as totacctbal \
from \
        ( \
                select \
                        substring(c_phone from 1 for 2) as cntrycode, \
                        c_acctbal \
                from \
                        customer \
                where \
                        substring(c_phone from 1 for 2) in \
                                ('13', '31', '23', '29', '30', '18', '17') \
                        and c_acctbal > ( \
                                select \
                                        avg(c_acctbal) \
                                from \
                                        customer \
                                where \
                                        c_acctbal > 0.00 \
                                        and substring(c_phone from 1 for 2) in \
                                                ('13', '31', '23', '29', '30', '18', '17') \
                        ) \
                        and not exists ( \
                                select \
                                        * \
                                from \
                                        orders \
                                where \
                                        o_custkey = c_custkey \
                        ) \
        ) as custsale \
group by \
        cntrycode \
order by \
        cntrycode; \
"

tmp1 = customer.rdd.getNumPartitions()
tmp2 = orders.rdd.getNumPartitions()

res = spark.sql(sqlString)
res.show()
spark.sql(sqlString).explain()
print("Partitions : Customer  Orders ",tmp1,tmp2)



