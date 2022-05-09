from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType

# 1 - Application Name - # of executors - # of cores specified here
spark = SparkSession.builder.appName("Query8_DFAPI").config("spark.executor.instances","4").config("spark.executor.cores","4").master("yarn").getOrCreate()


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

partSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/schema/part.csv").schema
part = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(partSchema).option("delimiter", "|").csv("/user/data/part.tbl")

customer.registerTempTable("customer")
orders.registerTempTable("orders")
lineitem.registerTempTable("lineitem")
supplier.registerTempTable("supplier")
nation.registerTempTable("nation")
region.registerTempTable("region")
part.registerTempTable("part")


sqlString="select \
        o_year, \
        sum(case \
                when nation = 'BRAZIL' then volume \
                else 0 \
        end) / sum(volume) as mkt_share \
from \
        ( \
                select \
                        extract(year from o_orderdate) as o_year, \
                        l_extendedprice * (1 - l_discount) as volume, \
                        n2.n_name as nation \
                from \
                        part, \
                        supplier, \
                        lineitem, \
                        orders, \
                        customer, \
                        nation n1, \
                        nation n2, \
                        region \
                where \
                        p_partkey = l_partkey \
                        and s_suppkey = l_suppkey \
                        and l_orderkey = o_orderkey \
                        and o_custkey = c_custkey \
                        and c_nationkey = n1.n_nationkey \
                        and n1.n_regionkey = r_regionkey \
                        and r_name = 'AMERICA' \
                        and s_nationkey = n2.n_nationkey \
                        and o_orderdate between date '1995-01-01' and date '1996-12-31' \
                        and p_type = 'ECONOMY ANODIZED STEEL' \
        ) as all_nations \
group by \
        o_year \
order by \
        o_year; \
"
tmp1 = lineitem.rdd.getNumPartitions()
tmp2 = orders.rdd.getNumPartitions()
tmp3 = customer.rdd.getNumPartitions()
tmp4 = supplier.rdd.getNumPartitions()
tmp5 = nation.rdd.getNumPartitions()
tmp6 = region.rdd.getNumPartitions()
tmp7 = part.rdd.getNumPartitions()
res = spark.sql(sqlString)
res.show()
spark.sql(sqlString).explain()
print("Partitions : Lineitem Orders Customer Supplier Nation Region Part",tmp1,tmp2,tmp3,tmp4,tmp5,tmp6,tmp7)


