HDFS DataNode Offline DiskBalancer
===============



本工具主要用于平衡 HDFS 各磁盘的使用率，避免单盘使用率过高。



### 两种使用场景：

* 磁盘平衡：平衡各个磁盘的使用率
* 磁盘下线：计划下线掉某个磁盘，需要将该盘的数据块，完全移至本节点其他磁盘存储中



### 使用方法

关闭节点上的DataNode 服务，切换到 hdfs 进程的用户下，一般为 `hdfs`.

````shell
su - hdfs
````



**查看帮助**

````shell
./hdfs-dn-diskbalancer.sh -h


Usage: hdfs-dn-diskbalancer.sh
        --hdfs-config|-c FILENAME  Specify the datanode's HDFS configuration file
                                   Default: /etc/ecm/hadoop-conf/hdfs-site.xml
        --threshold|-t PERCENTAGE  Tolerate up to this % of difference between 2 disks.
                                   Integer value. Default: 10
        --purge-volumes|-p DISKS_TO_PURGE
                                   Specify the comma-delimited disk(s) to be purged.
                                   eg. /mnt/disk1 or /mnt/disk1,/mnt/disk2
        --force|-f                 Force running when executed as root

````





**磁盘平衡**

````
./hdfs-dn-diskbalancer.sh
````





**磁盘下线**

如下线 disk1

````
./hdfs-dn-diskbalancer.sh -p /mnt/disk1
````





### 其他优化



* DataNode 使用 AvailableSpaceVolumeChoosingPolicy 策略

```` xml
<property>
    <name>dfs.datanode.fsdataset.volume.choosing.policy</name>
    <value>org.apache.hadoop.hdfs.server.datanode.fsdataset.AvailableSpaceVolumeChoosingPolicy</value>
</property>
````





* Hadoop 3+ 支持在线 DiskBalancer，可开启 DiskBalancer