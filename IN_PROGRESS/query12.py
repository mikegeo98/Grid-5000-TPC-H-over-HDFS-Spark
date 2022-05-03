from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType
from datetime import date, time, datetime

spark = SparkSession.builder.appName("Query1_DFAPI").getOrCreate()

fileSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/lineitem.csv").schema
lineitem = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(fileSchema).option("delimiter", "|").csv("/user/diplomma/data/data10/lineitem.tbl")

orderSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/orders.csv").schema
orders = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(orderSchema).option("delimiter", "|").csv("/user/diplomma/data/data10/orders.tbl")


orders.registerTempTable("orders")
lineitem.registerTempTable("lineitem")


#sqlString = "select * from region"
sqlString= "select \
        l_shipmode, \
        sum(case \
                when o_orderpriority = '1-URGENT' \
                        or o_orderpriority = '2-HIGH' \
                        then 1 \
                else 0 \
        end) as high_line_count, \
        sum(case \
                when o_orderpriority <> '1-URGENT' \
                        and o_orderpriority <> '2-HIGH' \
                        then 1 \
                else 0 \
        end) as low_line_count \
from \
        orders, \
        lineitem \
where \
        o_orderkey = l_orderkey \
        and l_shipmode in ('MAIL', 'SHIP') \
        and l_commitdate < l_receiptdate \
        and l_shipdate < l_commitdate \
        and l_receiptdate >= date '1994-01-01' \
        and l_receiptdate < date '1994-01-01' + interval '1' year \
group by \
        l_shipmode \
order by \
        l_shipmode; \
"
queryStartTime = datetime.now()
res = spark.sql(sqlString)
queryStopTime = datetime.now()
runTime = queryStopTime-queryStartTime 
res.show()

print("Runtime:", runTime)


