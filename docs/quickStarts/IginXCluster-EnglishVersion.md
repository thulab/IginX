# IginX Installation and Use Manual (Cluster)

[TOC]

IginX is is a new-generation highly scalable time series database distributed middleware, designed to meet industrial Internet scenarios. It was launched by Tsinghua University's National Engineering Laboratory of Big Data System Software. It currently supports IoTDB，InfluxDB as data backends.

## Download and Installation

### Java Installation

Since ZooKeeper, IginX and IoTDB are all developed using Java, Java needs to be installed first. If a running environment of JDK >= 1.8 has been installed locally, **skip this step entirely**.

1. First, visit the [official Java website] (https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html) to download the JDK package for your current system.

2. Installation

```shell
$ cd ~/Downloads
$ tar -zxf jdk-8u181-linux-x64.gz # unzip files
$ mkdir /opt/jdk
$ mv jdk-1.8.0_181 /opt/jdk/
```

3. Set the path

Edit the ~/.bashrc file and add the following two lines at the end of the file:

```shell
export JAVA_HOME = /usr/jdk/jdk-1.8.0_181
export PATH=$PATH:$JAVA_HOME/bin
```

Load the file with the changed configuration (into shell scripts):

```shell
$ source ~/.bashrc
```

4. Use java -version to determine whether JDK installed successfully.

```shell
$ java -version
java version "1.8.0_181"
Java(TM) SE Runtime Environment (build 1.8.0_181-b13)
Java HotSpot(TM) 64-Bit Server VM (build 25.181-b13, mixed mode)
```

If the words above are displayed, it means the installation was successful.

#### ZooKeeper Installation

 ZooKeeper is an open-source server for highly reliable distributed coordination of cloud applications, launched by Apache. If you need to deploy more than one instance of IginX, you will need to install ZooKeeper. Otherwise, **skip this step entirely**

The specific installation method is as follows,

1. Visit the [official website](https://zookeeper.apache.org/releases.html)to download and unzip ZooKeeper

```shell
$ cd ~
$ wget https://mirrors.bfsu.edu.cn/apache/zookeeper/zookeeper-3.7.0/apache-zookeeper-3.7.0-bin.tar.gz
$ tar -zxvf apache-zookeeper-3.7.0-bin.tar.gz
```

2. Modify the default ZooKeeper profile

```shell
$ cd apache-zookeeper-3.7.0-bin/
$ mkdir data
$ cp conf/zoo_sample.cfg conf/zoo.cfg
```

Then edit the conf/zoo.cfg file and

```shell
dataDir=/tmp/zookeeper
```

Modify to

```shell
dataDir=data
```

#### IoTDB Installation

IoTDB is Apache's Apache IoT native database with high performance for data management and analysis, deployable on the edge and the cloud.

The specific installation method is as follows:

```shell
$ cd ~
$ wget https://mirrors.bfsu.edu.cn/apache/iotdb/0.12.0/apache-iotdb-0.12.0-server-bin.zip
$ unzip apache-iotdb-0.12.0-server-bin.zip
```

### IginX Installation

Go directly to the [IginX project](https://github.com/thulab/IginX/) and download the [IginX project release package](https://github.com/thulab/IginX/releases/download/release%2Fv0.4.0/IginX-release-v0.4.0-bin.zip). That's it.


## Launch

Here is an example of starting one or two IginX instances and two IoTDB instances to demonstrate how to start an IginX cluster.

### Start multiple IoTDB instances

Here is an example of starting two instances with ports 6667 and 7667 on a single machine.

Modify the configuration file IoTDB_HOME/conf/iotdb-engine.properties

```shell
rpc_port=6667
```

Start the first instance:

```shell
$ cd ~
$ cd apache-iotdb-0.12.0-server-bin/
$ ./sbin/start-server.sh # start instance one 127.0.0.1:6667
```

Modify the configuration file conf/iotdb-engine.properties

```shell
rpc_port=7667
```

Start the second instance:

```shell
$ ./sbin/start-server.sh # Start instance two 127.0.0.1: 7667
```

### Start ZooKeeper

```shell
$ cd ~
$ cd apache-zookeeper-3.7.0-bin/
$ ./bin/zkServer.sh start
```

The following display of words means the ZooKeeper installation and launch was successful：

```shell
ZooKeeper JMX enabled by default
Using config: /home/root/apache-zookeeper-3.7.0-bin/bin/../conf/zoo.cfg
Starting zookeeper ... STARTED
```

### Start multiple IginX instances

Modify IginX_HOME/conf/config.Properties to join the two IoTDB instances that already started

```shell
storageEngineList=127.0.0.1#6667#iotdb#username=root#password=root#sessionPoolSize=100#dataDir=/path/to/your/data/,127.0.0.1#6688#iotdb#username=root#password=root# sessionPoolSize=100#dataDir=/path/to/your/data/

#Storage method selection ZooKeeper
metaStorage=zookeeper

# Provide ZooKeeper port
zookeeperConnectionString=127.0.0.1:2181

# Comment out file, etcd related configuration
# fileDataDir=meta
# etcdEndpoints=http://localhost:2379
```

Start the first IginX instance

```shell
$ cd ~
$ cd Iginx
$ chmod +x sbin/start_iginx.sh # Add startup permissions to the startup script
$ ./sbin/start_iginx.sh
````

Modify conf/config.Properties

```shell
# iginx binding port
port=7888
# rest bind port
restPort=7666
```

Launch a second instance of IginX.

```shell
$ ./sbin/start_iginx.sh
```

## Access IginX

### RESTful Interface

After the startup is complete, you can easily use the RESTful interface to write and query data to IginX.

Create the file insert.json and add the following to it:

```json
[
  {
    "name": "archive_file_tracked",
    "datapoints": [
        [1359788400000, 123.3],
        [1359788300000, 13.2],
        [1359788410000, 23.1 ]
    ],
    "tags": {
        "host": "server1",
        "data_center": "DC1"
    }
  },
  {
      "name": "archive_file_search",
      "timestamp": 1359786400000,
      "value": 321,
      "tags": {
          "host": "server2"
      }
  }
]
```

Insert data into the database from an IginX instance using the following command:

```shell
$ curl -XPOST -H'Content-Type: application/json' -d @insert.json http://127.0.0.1:6666/api/v1/datapoints
```

After inserting data, you can also query the data just written using the RESTful interface.

Create a file query.json and write the following data to it:

```json
{
"start_absolute" : 1,
"end_relative": {
"value": "5",
"unit": "days"
},
"time_zone": "Asia/Kabul",
"metrics": [
{
"name": "archive_file_tracked"
},
{
"name": "archive_file_search"
}
]
}
```

Use the following command to query data from IginX instance two:

```shell
$ curl -XPOST -H'Content-Type: application/json' -d @query.json http://127.0.0.1:6666/api/v1/datapoints/query
```

The command will return information about the data point just inserted:

```json
{
    "queries": [
        {
            "sample_size": 3,
            "results": [
                {
                    "name": "archive_file_tracked",
                    "group_by": [
                        {
                            "name": "type",
                            "type": "number"
                        }
                    ],
                    "tags": {
                        "data_center": [
                            "DC1"
                        ],
                        "host": [
                            "server1"
                        ]
                    },
                    "values": [
                        [
                            1359788300000,
                            13.2
                        ],
                        [
                            1359788400000,
                            123.3
                        ],
                        [
                            1359788410000,
                            23.1
                        ]
                    ]
                }
            ]
        },
        {
            "sample_size": 1,
            "results": [
                {
                    "name": "archive_file_search",
                    "group_by": [
                        {
                            "name": "type",
                            "type": "number"
                        }
                    ],
                    "tags": {
                        "host": [
                            "server2"
                        ]
                    },
                    "values": [
                        [
                            1359786400000,
                            321.0
                        ]
                    ]
                }
            ]
        }
    ]
}
```

For more interfaces, please refer to the official IginX manual.

### RPC Interface

In addition to the RESTful interface, IginX also provides RPC data access interface. For that specific interface, please refer to the official[IginX Official Manual](https://github.com/thulab/IginX/blob/main/docs/pdf/userManualC.pdf). At the same time, IginX also provides some [official examples](https://github.com/thulab/IginX/tree/main/example/src/main/java/cn/edu/tsinghua/iginx/session), showing the most common usage of the RPC interface.

Below is a short tutorial on how to use it.

Since the IginX 0.4 version has not been released to the maven central repository, if you want to use it, you need to manually install it to the local maven repository. The specific installation method is as follows:

```shell
# Download IginX 0.4 rc version source package
$ wget https://github.com/thulab/IginX/archive/refs/tags/release/v0.4.0.tar.gz
# Unzip the source package
$ tar -zxvf v0.4.0.tar.gz
# Enter the project's main directory
$ cd IginX-release-v0.4.0
# Install to local maven repository
$ mvn clean install -DskipTests
````

Only when you are using it, you need to introduce the following dependencies in the pom file of the corresponding project:

```xml
<dependency>
  <groupId>cn.edu.tsinghua</groupId>
  <artifactId>iginx-core</artifactId>
  <version>0.5.0-SNAPSHOT</version>
</dependency>
```

Before accessing IginX, you first need to open a session and try to connect. The session constructor has 4 parameters, which are the ip and port of IginX to connect to, and the username and password for IginX authentication. The current authentication system is still being written, so the account name and password to access the backend IginX can directly fill in root:

```Java
Session session = new Session("127.0.0.1", 6888, "root", "root");
session.openSession();
```

You can then try to insert data into IginX. Since IginX supports the creation of time series when data is written for the first time, there is no need to call the relevant series creation interface in advance. IginX provides a row-style and column-style data-writing interface. The following is a usage example of the column-style data writing interface:

```java
private static void insertColumnRecords(Session session) throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add("sg.d1.s1");
        paths.add("sg.d2.s2");
        paths.add("sg.d3.s3");
        paths.add("sg.d4.s4");

        int size = 1500;
        long[] timestamps = new long[size];
        for (long i = 0; i < size; i++) {
            timestamps[(int) i] = i;
        }

        Object[] valuesList = new Object[4];
        for (long i = 0; i < 4; i++) {
            Object[] values ​​= new Object[size];
            for (long j = 0; j < size; j++) {
                if (i < 2) {
                  values[(int) j] = i + j;
                } else {
                  values[(int) j] = RandomStringUtils.randomAlphanumeric(10).getBytes();
                }
            }
            valuesList[(int) i] = values;
        }

        List<DataType> dataTypeList = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            dataTypeList.add(DataType.LONG);
        }
        for (int i = 0; i < 2; i++) {
            dataTypeList.add(DataType.BINARY);
        }

        session.insertColumnRecords(paths, timestamps, valuesList, dataTypeList, null);
}
```

After completing the data writing, you can use the data query interface to query the data just written:

```java
private static void queryData(Session session) throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add("sg.d1.s1");
        paths.add("sg.d2.s2");
        paths.add("sg.d3.s3");
        paths.add("sg.d4.s4");

        long startTime = 100L;
        long endTime = 200L;

        SessionQueryDataSet dataSet = session.queryData(paths, startTime, endTime);
        dataSet.print();
}
```

You can also use the downsampling aggregation query interface to query the interval statistics of the data:

```java
private static void downsampleQuery(Session session) throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add("sg.d1.s1");
        paths.add("sg.d2.s2");

        long startTime = 100L;
        long endTime = 1101L;

        //MAX
        SessionQueryDataSet dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.MAX, 100);
        dataSet.print();

        // MIN
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.MIN, ROW_INTERVAL * 100);
        dataSet.print();

        //FIRST
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.FIRST, ROW_INTERVAL * 100);
        dataSet.print();

        // LAST
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.LAST, ROW_INTERVAL * 100);
        dataSet.print();

        // COUNT
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.COUNT, ROW_INTERVAL * 100);
        dataSet.print();

        // SUM
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.SUM, ROW_INTERVAL * 100);
        dataSet.print();

        // AVG
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.AVG, ROW_INTERVAL * 100);
        dataSet.print();

}

```

After the session is completed, you need to manually close and release your connection from your terminal/backend:

```shell
session.closeSession();
```

For the full version of the code, please refer to: https://github.com/thulab/IginX/blob/main/example/src/main/java/cn/edu/tsinghua/iginx/session/IoTDBSessionExample.java
