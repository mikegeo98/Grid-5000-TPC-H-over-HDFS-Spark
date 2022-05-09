from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType

# 1 - Application Name - # of executors - # of cores specified here
spark = SparkSession.builder.appName("Query21_DFAPI").config("spark.executor.instances","4").config("spark.executor.cores","4").master("yarn").getOrCreate()


spark.conf.set("spark.sql.join.preferSortMergeJoin", False)

# 2 - File Location

#spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

supplierSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/supplier.csv").schema
supplier = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(supplierSchema).option("delimiter", "|").csv("/user/data/supplier.tbl")

fileSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/lineitem.csv").schema
lineitem = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(fileSchema).option("delimiter", "|").csv("/user/data/lineitem.tbl")

orderSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/orders.csv").schema
orders = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(orderSchema).option("delimiter", "|").csv("/user/data/orders.tbl")

nationSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/nation.csv").schema
nation = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(nationSchema).option("delimiter", "|").csv("/user/data/nation.tbl")

supplier.registerTempTable("supplier")
orders.registerTempTable("orders")
lineitem.registerTempTable("lineitem")
nation.registerTempTable("nation")



sqlString="select \
        s_name, \
        count(*) as numwait \
from \
        supplier, \
        lineitem l1, \
        orders, \
        nation \
where \
        s_suppkey = l1.l_suppkey \
        and o_orderkey = l1.l_orderkey \
        and o_orderstatus = 'F' \
        and l1.l_receiptdate > l1.l_commitdate \
        and exists ( \
                select \
                        * \
                from \
                        lineitem l2 \
                where \
                        l2.l_orderkey = l1.l_orderkey \
                        and l2.l_suppkey <> l1.l_suppkey \
        ) \
        and not exists ( \
                select \
                        * \
                from \
                        lineitem l3 \
                where \
                        l3.l_orderkey = l1.l_orderkey \
                        and l3.l_suppkey <> l1.l_suppkey \
                        and l3.l_receiptdate > l3.l_commitdate \
        ) \
        and s_nationkey = n_nationkey \
        and n_name = 'SAUDI ARABIA' \
group by \
        s_name \
order by \
        numwait desc, \
        s_name; \
"

tmp1 = lineitem.rdd.getNumPartitions()
tmp2 = orders.rdd.getNumPartitions()
tmp3 = supplier.rdd.getNumPartitions()
tmp5 = nation.rdd.getNumPartitions()
res = spark.sql(sqlString)
res.show()
spark.sql(sqlString).explain()
print("Partitions : Lineitem Orders Supplier Nation",tmp1,tmp2,tmp3,tmp5)



