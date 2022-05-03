from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType

spark = SparkSession.builder.appName("Query7_DFAPI").getOrCreate()

customerSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/customer.csv").schema
customer = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(customerSchema).option("delimiter", "|").csv("/user/diplomma/data/data/customer.tbl")

fileSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/lineitem.csv").schema
lineitem = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(fileSchema).option("delimiter", "|").csv("/user/diplomma/data/data/lineitem.tbl")

orderSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/orders.csv").schema
orders = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(orderSchema).option("delimiter", "|").csv("/user/diplomma/data/data/orders.tbl")

supplierSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/supplier.csv").schema
supplier = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(supplierSchema).option("delimiter", "|").csv("/user/diplomma/data/data/supplier.tbl")

nationSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/nation.csv").schema
nation = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(nationSchema).option("delimiter", "|").csv("/user/diplomma/data/data/nation.tbl")

regionSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/region.csv").schema
region = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(regionSchema).option("delimiter", "|").csv("/user/diplomma/data/data/region.tbl")


customer.registerTempTable("customer")
orders.registerTempTable("orders")
lineitem.registerTempTable("lineitem")
supplier.registerTempTable("supplier")
nation.registerTempTable("nation")
region.registerTempTable("region")


sqlString="select \
        supp_nation, \
        cust_nation, \
        l_year, \
        sum(volume) as revenue \
from \
        ( \
                select \
                        n1.n_name as supp_nation, \
                        n2.n_name as cust_nation, \
                        extract(year from l_shipdate) as l_year, \
                        l_extendedprice * (1 - l_discount) as volume \
                from \
                        supplier, \
                        lineitem, \
                        orders, \
                        customer, \
                        nation n1,\
                        nation n2 \
                where \
                        s_suppkey = l_suppkey \
                        and o_orderkey = l_orderkey \
                        and c_custkey = o_custkey \
                        and s_nationkey = n1.n_nationkey \
                        and c_nationkey = n2.n_nationkey \
                        and ( \
                                (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') \
                                or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE') \
                        ) \
                        and l_shipdate between date '1995-01-01' and date '1996-12-31' \
        ) as shipping \
group by \
        supp_nation, \
        cust_nation, \
        l_year \
order by \
        supp_nation, \
        cust_nation, \
        l_year; \
"

res = spark.sql(sqlString)
res.show()


