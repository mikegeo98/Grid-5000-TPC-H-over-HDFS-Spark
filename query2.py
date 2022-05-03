from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType
from datetime import date, time, datetime

#spark = SparkSession.builder.appName("Query2_DFAPI").master("yarn").getOrCreate()#master("yarn").getOrCreate()
#spark.conf.set("spark.sql.join.preferSortMergeJoin", False)
spark = SparkSession.builder.appName("Query2_DFAPI").config("spark.executor.instances","4").config("spark.executor.cores","4").master("yarn").getOrCreate()
#spark = SparkSession.builder.appName("Query2_DFAPI").config("spark.yarn.am.memory","40960").config("spark.executor.memory","4g").config("spark.executor.pyspark.memory","2g").config("spark.executor.instances", "6").master("yarn").getOrCreate()
spark.conf.set("spark.sql.join.preferSortMergeJoin", False)
print("We are here")


partSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/part.csv").schema
part = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(partSchema).option("delimiter", "|").csv("/user/diplomma/data/data100/part.tbl")
part.registerTempTable("part")

supplierSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/supplier.csv").schema
supplier = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(supplierSchema).option("delimiter", "|").csv("/user/diplomma/data/data100/supplier.tbl")
supplier.registerTempTable("supplier")

partsuppSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/partsupp.csv").schema
partsupp = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(partsuppSchema).option("delimiter", "|").csv("/user/diplomma/data/data100/partsupp.tbl")
partsupp.registerTempTable("partsupp")

nationSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/nation.csv").schema
nation = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(nationSchema).option("delimiter", "|").csv("/user/diplomma/data/data100/nation.tbl")
nation.registerTempTable("nation")

regionSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/region.csv").schema
region = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(regionSchema).option("delimiter", "|").csv("/user/diplomma/data/data100/region.tbl")
region.registerTempTable("region")

sqlString="select \
        s_acctbal, \
        s_name, \
        n_name, \
        p_partkey, \
        p_mfgr, \
        s_address, \
        s_phone, \
        s_comment \
from \
        part, \
        supplier, \
        partsupp, \
        nation, \
        region \
where \
        p_partkey = ps_partkey \
        and s_suppkey = ps_suppkey \
        and p_size = 15 \
        and p_type like '%BRASS' \
        and s_nationkey = n_nationkey \
        and n_regionkey = r_regionkey \
        and r_name = 'EUROPE' \
        and ps_supplycost = ( \
                select \
                        min(ps_supplycost) \
                from \
                        partsupp, \
                        supplier, \
                        nation, \
                        region \
                where \
                        p_partkey = ps_partkey \
                        and s_suppkey = ps_suppkey \
                        and s_nationkey = n_nationkey \
                        and n_regionkey = r_regionkey \
                        and r_name = 'EUROPE' \
order by \
        s_acctbal desc, \
        n_name, \
        s_name, \
        p_partkey; \
"
#lz = part.rdd.glom().map(len).collect()
#l = partsupp.rdd.glom().map(len).collect()  # get length of each partition
#l1 = nation.rdd.glom().map(len).collect()
#l2 = region.rdd.glom().map(len).collect()  # get length of each partition
#l3 = supplier.rdd.glom().map(len).collect()

#print('Min Parition Size: ',min(l),'. Max Parition Size: ', max(l),'. Avg Parition Size: ', sum(l)/len(l),'. Total Partitions: ', len(l))
tmp = region.rdd.getNumPartitions()
tmp2 = nation.rdd.getNumPartitions()
tmp3 = part.rdd.getNumPartitions()
tmp4 = partsupp.rdd.getNumPartitions()
tmp5 = supplier.rdd.getNumPartitions()
queryStartTime = datetime.now()
res = spark.sql(sqlString)
queryStopTime = datetime.now()
runTime = queryStopTime-queryStartTime 
res.show()

print("Runtime: ",runTime)
spark.sql(sqlString).explain()
print("Region partitions: ", tmp)
print("Nation partitions: ", tmp2)
print("Part partitions: ", tmp3)
print("Partsupp partitions: ", tmp4)
print("Supplier partitions: ", tmp5)

#print('Min Partition Size: ', min(lz), '. Max Partition Size: ', max(lz), '. Avg Partition Size: ', sum(lz)/len(lz), '. Total Partitions: ', len(lz))
#print('Min Parition Size: ',min(l),'. Max Parition Size: ', max(l),'. Avg Parition Size: ', sum(l)/len(l),'. Total Partitions: ', len(l))
#print('Min Partition Size: ', min(l1), '. Max Partition Size: ', max(l1), '. Avg Partition Size: ', sum(l1)/len(l1), '. Total Partitions: ', len(l1))
#print('Min Parition Size: ',min(l2),'. Max Parition Size: ', max(l2),'. Avg Parition Size: ', sum(l2)/len(l2),'. Total Partitions: ', len(l2))
#print('Min Partition Size: ', min(l3), '. Max Partition Size: ', max(l3), '. Avg Partition Size: ', sum(l3)/len(l3), '. Total Partitions: ', len(l3))
