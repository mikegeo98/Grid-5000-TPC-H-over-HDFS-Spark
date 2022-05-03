from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType

spark = SparkSession.builder.appName("Query4_DFAPI").master("yarn").getOrCreate()

fileSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/lineitem.csv").schema
lineitem = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(fileSchema).option("delimiter", "|").csv("/user/diplomma/data/data/lineitem.tbl")

orderSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/orders.csv").schema
orders = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(orderSchema).option("delimiter", "|").csv("/user/diplomma/data/data/orders.tbl")

lineitem.registerTempTable("lineitem")
orders.registerTempTable("orders")

sqlString="select \
        o_orderpriority, \
        count(*) as order_count \
from \
        orders \
where \
        o_orderdate >= date '1993-07-01' \
        and o_orderdate < date '1993-07-01' + interval '3' month \
        and exists ( \
                select \
                        * \
                from \
                        lineitem \
                where \
                        l_orderkey = o_orderkey \
                        and l_commitdate < l_receiptdate \
        ) \
group by \
        o_orderpriority \
order by \
        o_orderpriority; \
"
res = spark.sql(sqlString)
res.show()


