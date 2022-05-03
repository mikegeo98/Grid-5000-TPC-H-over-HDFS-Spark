from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType

spark = SparkSession.builder.appName("Query1_DFAPI").getOrCreate()

customerSchema = spark.read.options(header="true", inferSchema = "true").csv("/home/mikeg/Documents/databases/TPC-H/TPC-H_Tools_v3.0.0/dbgen/tmpqueries/schema/customer.csv").schema
customer = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(customerSchema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/customer.tbl")

orderSchema = spark.read.options(header="true", inferSchema = "true").csv("/home/mikeg/Documents/databases/TPC-H/TPC-H_Tools_v3.0.0/dbgen/tmpqueries/schema/orders.csv").schema
orders = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(orderSchema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/orders.tbl")

customer.registerTempTable("customer")
orders.registerTempTable("orders")


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



res = spark.sql(sqlString)
res.show()


