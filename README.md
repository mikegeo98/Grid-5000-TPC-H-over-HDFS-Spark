# Grid-5000-TPC-H-over-HDFS-Spark
Experiments in the Grid '5000 platform, using the TPC-H dataset and queries, Spark SQL for querying, and HDFS for distributed storage.

## Cluster Setup

### Renting Disks

First, we have to rent an amount of persistent storage on each node, where our data will be stored.
In this example, we rent an HDD and an SSD from 6 different nodes of the cluster Parasilo, for 12 hours.
```
oarsub -t noop -l {"type='disk' and host='parasilo-1.rennes.grid5000.fr' and disk in ('disk1.parasilo-1', 'disk5.parasilo-1')"}/host=1/disk=2,walltime=12

oarsub -t noop -l {"type='disk' and host='parasilo-10.rennes.grid5000.fr' and disk in ('disk1.parasilo-10', 'disk5.parasilo-10')"}/host=1/disk=2,walltime=12

oarsub -t noop -l {"type='disk' and host='parasilo-15.rennes.grid5000.fr' and disk in ('disk1.parasilo-15', 'disk5.parasilo-15')"}/host=1/disk=2,walltime=12

oarsub -t noop -l {"type='disk' and host='parasilo-18.rennes.grid5000.fr' and disk in ('disk1.parasilo-18', 'disk5.parasilo-18')"}/host=1/disk=2,walltime=12

oarsub -t noop -l {"type='disk' and host='parasilo-19.rennes.grid5000.fr' and disk in ('disk1.parasilo-19', 'disk5.parasilo-19')"}/host=1/disk=2,walltime=12

oarsub -t noop -l {"type='disk' and host='parasilo-9.rennes.grid5000.fr' and disk in ('disk1.parasilo-9', 'disk5.parasilo-9')"}/host=1/disk=2,walltime=12
```

### Renting Nodes

After renting the disks, we move on to rent the respective cluster nodes, to use their computing and storage resources.
```
oarsub -I -t allow_classic_ssh -l {"host in ('parasilo-1.rennes.grid5000.fr','parasilo-10.rennes.grid5000.fr','parasilo-15.rennes.grid5000.fr','parasilo-18.rennes.grid5000.fr','parasilo-19.rennes.grid5000.fr','parasilo-9.rennes.grid5000.fr')"}/host=6,walltime=11
```

### Mount Filesystems on Disks

For each one of the disks, the following procedure will have to be followed (here for disk 1).

With the command ```lsblk ``` information about available block devices is displayed.

Use the following command to gain some root privileges.
```
sudo-g5k -i
```
With the next command the full paths to each disk volume are shown
```
ls -l /dev/disk/by-path/
```
Copy the full path of the necessary disk, and use the cfdisk command
```
cfdisk /dev/disk/by-path/pci-0000:03:00.0-scsi-0:0:1:0
```
Select ```GPT```, create a New Partition with size equal to the disk, write partition to disk, and then hit quit.

With ```lsblk``` you can now see that a partition is created on disk 1, and with the following command (again by copying the full path of the partition), a filesystem is created on the new partition.
```
mkfs.ext4 -m 0 /dev/disk/by-path/pci-0000:03:00.0-scsi-0:0:1:0-part1
```

With the following commands the filesystem is mounted inside the folder /mnt/mylocaldisk1, and can now be used.
```
mkdir -p /mnt/mylocaldisk1
mount /dev/disk/by-path/pci-0000:03:00.0-scsi-0:0:1:0-part1 /mnt/mylocaldisk1
cd /mnt/mylocaldisk1
mkdir tmp
sudo-g5k chmod 777 /mnt/mylocaldisk1/tmp
```

This procedure is then repeated for all necessary disks on all nodes.

### Hadoop - Spark - Yarn Configuration

The following files have to be modified:
/path/to/hadoop-3.2.2/etc/hadoop/core-site.xml, /path/to/spark/conf/core-site.xml : the master node, and the hadoop directory are specified.

```
<configuration>
<property>
  <name>hadoop.tmp.dir</name>
  <value>/mnt/mylocaldisk1/tmp</value>
</property>
<property>
    <name>fs.defaultFS</name>
    <value>hdfs://parasilo-1.rennes.grid5000.fr:9000</value>
</property>
</configuration>
```

/path/to/hadoop-3.2.2/etc/hadoop/yarn-site.xml , /path/to/spark/conf/yarn-site.xml: to specify the resource manager node and ports

```
<property>
  <name>yarn.resourcemanager.address</name>
  <value>parasilo-1:8032</value>
</property>
<property>
  <name>yarn.resourcemanager.scheduler.address</name>
  <value>parasilo-1:8030</value>
</property>
<property>
<name>yarn.resourcemanager.resource-tracker.address</name>
<value>parasilo-1:8031</value>
</property>
```

hdfs-site.xml


```
<configuration>
    <property>
        <name>dfs.data.dir</name>
        <value>/mnt/mylocaldisk1/tmp/datanode</value>
    </property>
    <property>
        <name>dfs.name.dir</name>
        <value>/mnt/mylocaldisk1/tmp/namenode</value>
    </property>
    <property>
        <name>dfs.replication</name>
        <value>3</value>
    </property>

</configuration>
```


```
source /path/to/hadoop.txt
source /path/to/spark.txt
source /path/to/hive.txt
```

```
uniq $OAR_NODEFILE $HADOOP_HOME/etc/hadoop/dfs.hosts
uniq $OAR_NODEFILE $HADOOP_HOME/etc/hadoop/workers
```
Format the namenode:
```
hdfs namenode -format
```
Launch namenode, datanodes, nodemanagers, resourcemanager:
```
cd $HADOOP_HOME/sbin
./start-all.sh
```
Launch Spark master:
```
cd $SPARK_HOME/sbin
./start-master.sh
```
Check that everything went well

```
hdfs dfsadmin -report
```

## Generate Data and Queries

Download the TPC-H generation tool from GitHub with the following command
```
git clone https://github.com/electrum/tpch-dbgen.git
cd tpch-dbgen
```

Inside the dbgen repository, execute
```
./dbgen -s SIZE
```

where SIZE will equal the total dataset size that you prefer.

Move all the data tables in a new folder.

mkdir data/
mv *.tbl data/

Similarly, the 22 TPC-H queries are generated using the command
```
./qgen
```

## Insert data into datanodes

First, create a directory where the data will be stored
```
hdfs dfs -mkdir /user
```

You can verify that everything went well with:
```
hdfs dfs -ls /
```
Copy data and schema to HDFS
```
hdfs dfs -copyFromLocal /path/to/data /user
hdfs dfs -copyFromLocal /path/to/schema /user
```

## Running Spark SQL queries

You can run a given query with Spark SQL using spark-submit:
```
spark-submit query.py
```
Inside the .py file, you can specify a number of Spark paramaters, like the number of Spark executors that will be used, or the # of executor cores inside each executor.

In the TPC_H_Queries folder you can find .py files with SQL queries from the TPC-H benchmark.



### Useful Commands

To see which nodes are rented
```
uniq $OAR_NODEFILE 
```

To switch nodes
```
oarsh NODE_NAME (e.g. parasilo-19)
```

To extend the duration of a reservation
```
oarwalltime 1885968 +9:00
```
Before renting nodes, you can use this command to see how you do in terms of Grid'5000 time constraints.
```
usagepolicycheck -t
``` 
