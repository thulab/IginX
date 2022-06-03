# IginX Installation Manual (Compilation and Installation)

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

### Maven Installation

Maven is a build automation tool used primarily to build and managa Java projects. If you need to compile from the source code, you also need to install a Maven environment >= 3.6. Otherwise, **skip this step entirely**.

1. Visit the [official website](http://maven.apache.org/download.cgi)to download and unzip Maven

```shell
$ wget http://mirrors.hust.edu.cn/apache/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz
$ tar -xvf  apache-maven-3.3.9-bin.tar.gz
$ sudo mv -f apache-maven-3.3.9 /usr/local/
```

2. Set the path

Edit the ~/.bashrc file and add the following two lines at the end of the file:

```shell
export MAVEN_HOME=/usr/local/apache-maven-3.3.9
export PATH=${PATH}:${MAVEN_HOME}/bin
```

Load the file with the changed configuration (into shell scripts):

```shell
$ source ~/.bashrc
```

3. Type mvn -v to determine whether Maven installed successfully.

```shell
$ mvn -v
Apache Maven 3.6.1 (d66c9c0b3152b2e69ee9bac180bb8fcc8e6af555; 2019-04-05T03:00:29+08:00)
```

If the words above are displayed, that means the installation was successful.

### IoTDB Installation

IoTDB is Apache's Apache IoT native database with high performance for data management and analysis, deployable on the edge and the cloud.

The specific installation method is as follows:

```shell
$ cd ~
$ wget https://mirrors.bfsu.edu.cn/apache/iotdb/0.12.0/apache-iotdb-0.12.0-server-bin.zip
$ unzip apache-iotdb-0.12.0-server-bin.zip
```

### IginX 

Compile with source code. If you need to modify code yourself, you can use this installation method. 

#### Compilation with source code

Fetch the latest development version and build it locally.

```shell
$ cd ~
$ git clone git@github.com:thulab/IginX.git
$ cd IginX
$ mvn clean install -Dmaven.test.skip=true
$ mvn package -pl core -Dmaven.test.skip=true
```

The following words are displayed, indicating that the IginX build is successful:

```shell
[INFO] Reactor Summary for IginX 0.1.0-SNAPSHOT:
[INFO]
[INFO] IginX .............................................. SUCCESS [  0.252 s]
[INFO] IginX Thrift ....................................... SUCCESS [  5.961 s]
[INFO] IginX Core ......................................... SUCCESS [  4.383 s]
[INFO] IginX IoTDB ........................................ SUCCESS [  0.855 s]
[INFO] IginX InfluxDB ..................................... SUCCESS [  0.772 s]
[INFO] IginX Client ....................................... SUCCESS [  7.713 s]
[INFO] IginX Example ...................................... SUCCESS [  0.677 s]
[INFO] IginX Test ......................................... SUCCESS [  0.114 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  20.887 s
[INFO] Finished at: 2021-07-12T16:01:31+08:00
[INFO] ------------------------------------------------------------------------
```

Additionally, IginX supports Docker. Use the following command to build a local IginX image:

```shell
mvn clean package -pl core -DskipTests docker:build
```

This may not work, which is not an immediate issue because you don't need Docker for IginX installation.

## Launch

### IoTDB 

First of all, you need to launch IoTDB.

```shell
$ cd ~
$ cd apache-iotdb-0.12.0-server-bin/
$ ./sbin/start-server.sh
```

The following display of words means the IoTDB installation was successful：

```shell
2021-05-27 08:21:07,440 [main] INFO  o.a.i.d.s.t.ThriftService:125 - IoTDB: start RPC ServerService successfully, listening on ip 0.0.0.0 port 6667
2021-05-27 08:21:07,440 [main] INFO  o.a.i.db.service.IoTDB:129 - IoTDB is set up, now may some sgs are not ready, please wait several seconds...
2021-05-27 08:21:07,448 [main] INFO  o.a.i.d.s.UpgradeSevice:109 - finish counting upgrading files, total num:0
2021-05-27 08:21:07,449 [main] INFO  o.a.i.d.s.UpgradeSevice:74 - Waiting for upgrade task pool to shut down
2021-05-27 08:21:07,449 [main] INFO  o.a.i.d.s.UpgradeSevice:76 - Upgrade service stopped
2021-05-27 08:21:07,449 [main] INFO  o.a.i.db.service.IoTDB:146 - Congratulation, IoTDB is set up successfully. Now, enjoy yourself!
2021-05-27 08:21:07,450 [main] INFO  o.a.i.db.service.IoTDB:93 - IoTDB has started.
```

### IginX

Using source code to launch

```shell
$ cd ~
$ cd Iginx
$ chmod +x startIginX.sh # enable permissions for startup scripts
$ ./startIginX.sh
```

The following display of words means the IginX installation was successful：

```shell
May 27, 2021 8:32:19 AM org.glassfish.grizzly.http.server.NetworkListener start
INFO: Started listener bound to [127.0.0.1:6666]
May 27, 2021 8:32:19 AM org.glassfish.grizzly.http.server.HttpServer start
INFO: [HttpServer] Started.
08:32:19.446 [Thread-0] INFO cn.edu.tsinghua.iginx.rest.RestServer - Iginx REST server has been available at http://127.0.0.1:6666/.
```

## Visit IginX

### RESTful Interface

After the startup is complete, you can easily use the RESTful interface to write and query data to IginX.

Create a file insert.json and add the following into it:

```json
[
  {
    "name": "archive_file_tracked",
    "datapoints": [
        [1359788400000, 123.3],
        [1359788300000, 13.2 ],
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

Insert data into the database using the following command:

```shell
$ curl -XPOST -H'Content-Type: application/json' -d @insert.json http://127.0.0.1:6666/api/v1/datapoints
```

After inserting data, you can also query the data just written using the RESTful interface.

Create a file query.json and write the following data into it:

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

Use the following command to query the data:

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

If you see the following information returned, it means you are able to successfully use RESTful interface to write and query data to IginX.

For more interfaces, please refer to the official [IginX manual](https://github.com/thulab/IginX/blob/main/docs/pdf/userManualC.pdf).

If you want to use a different interface, there is another option.

In addition to the RESTful interface, IginX also provides the RPC data access interface. For this specific interface, please refer to the official [IginX manual](https://github.com/thulab/IginX/blob/main/docs/pdf/userManualC.pdf).

At the same time, IginX also provides some official examples, showing the most common usage of the RPC interface.

Below is a short tutorial on how to use it.

### RPC Interface

Since the IginX 0.2 version has not been released to the Maven central repository, if you want to use it, you need to manually install it to the local Maven repository. 

The specific installation method is as follows:

```shell
# download iginx 0.2 rc version source code package
$ wget https://github.com/thulab/IginX/archive/refs/tags/rc/v0.2.0.tar.gz 
# Unzip the source package
$ tar -zxvf v0.2.0.tar.gz
# go to the main project's directory
$ cd IginX-rc-v0.2.0
# Install to local Maven repository
$ mvn clean install -DskipTests
```

Specifically, when using it, you only need to introduce the following dependencies in the pom file of the corresponding project:

```xml
<dependency>
    <groupId>cn.edu.tsinghua</groupId>
    <artifactId>iginx-core</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

Before accessing Iginx, you first need to create a session and try to connect. The Session constructor has 4 parameters, which are the ip and port IginX will to connect to, and the username and password for IginX authentication. The current authentication system is still being written, so the account name and password to access the backend IginX can directly fill in root:

```Java
Session session = new Session("127.0.0.1", 6888, "root", "root");
session.openSession();
```

You can then try to insert data into IginX. Since IginX supports the creation of time-series when data is written for the first time, there is no need to call the relevant series creation interface in advance. IginX provides row-style and column-style data writing interfaces. 

The following is an example of using the column-style data writing interface:


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
            Object[] values = new Object[size];
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

        // MAX
        SessionQueryDataSet dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.MAX, 100);
        dataSet.print();

        // MIN
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.MIN, ROW_INTERVAL * 100);
        dataSet.print();

        // FIRST
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



----------------------------------------------------------------------------------------------------------------------------------------

### Cluster

IginX now has two method options: to use ZooKeeper storage or write local files. 

When the deployment scenario is multiple IginX instances, you must use ZooKeeper storage, deploy a single IginX instance and use the source code compilation and installation method. Both can be selected, just change the corresponding configuration file.

Take two IoTDB instances and two IginX instances as examples.

#### Launching multiple IoTDB instances

Here is an example of starting two instances with ports 6667 and 7667, respectively, on a single machine.

```shell
$ cd ~
$ cd apache-iotdb-0.12.0-server-bin/
$ ./sbin/start-server.sh # 启动实例一 127.0.0.1: 6667
```

Modify the configuration file IoTDB_HOME/conf/iotdb-engine.properties

```shell
rpc_port=7667
```

Start the second instance.

```shell
$ ./sbin/start-server.sh # 启动实例二 127.0.0.1: 7667
```

#### Starting multiple IginX instances

Modify IginX_HOME/conf/config. Properties

```shell
storageEngineList=127.0.0.1#6667#iotdb#username=root#password=root#sessionPoolSize=100#dataDir=/path/to/your/data/,127.0.0.1#6688#iotdb#username=root#password=root#sessionPoolSize=100#dataDir=/path/to/your/data/

#存储方式选择 ZooKeeper
metaStorage=zookeeper 

# 提供ZooKeeper端口
zookeeperConnectionString=127.0.0.1:2181

#注释掉file、etcd相关配置
#fileDataDir=meta
#etcdEndpoints=http://localhost:2379
```

Start the first IginX instance.

```shell
$ cd ~
$ cd Iginx
$ chmod +x sbin/start_iginx.sh # 为启动脚本添加启动权限
$ ./sbin/start_iginx.sh
```

Modify conf/config. Properties

```shell
# iginx 绑定的端口
port=7888
# rest 绑定的端口
restPort=7666
```

Launch a second instance of IginX.

```shell
$ ./sbin/start_iginx.sh
```

### Configuration Items

In order to facilitate installation and management of IginX, IginX provides users with several optional item configurations. The IginX configuration file is located in `config.properties` under the `$IginX_HOME/conf` folder of the IginX installation directory. It mainly includes three aspects of configuration: IginX, Rest, and metadata management.

#### IginX Configuration

| Configuration Item                       | Description                                | 默认值                                                       |
| ---------------------------- | ------------------------------------- | ------------------------------------------------------------ |
| ip                           | iginx 绑定的 ip                       | 0.0.0.0                                                      |
| port                         | iginx 绑定的端口                      | 6888                                                         |
| username                     | iginx 本身的用户名                    | root                                                         |
| password                     | iginx 本身的密码                      | root                                                         |
| storageEngineList            | 时序数据库列表，使用','分隔不同实例   | 127.0.0.1#6667#iotdb#username=root#password=root#sessionPoolSize=100#dataDir=/path/to/your/data/ |
| maxAsyncRetryTimes           | 异步请求最大重复次数                  | 3                                                            |
| asyncExecuteThreadPool       | 异步执行并发数                        | 20                                                           |
| syncExecuteThreadPool        | 同步执行并发数                        | 60                                                           |
| replicaNum                   | 写入的副本个数                        | 1                                                            |
| databaseClassNames           | 底层数据库类名，使用','分隔不同数据库 | iotdb=cn.edu.tsinghua.iginx.iotdb.IoTDBPlanExecutor,influxdb=cn.edu.tsinghua.iginx.influxdb.InfluxDBPlanExecutor |
| policyClassName              | 策略类名                              | cn.edu.tsinghua.iginx.policy.NativePolicy                    |
| statisticsCollectorClassName | 统计信息收集类                        | cn.edu.tsinghua.iginx.statistics.StatisticsCollector         |
| statisticsLogInterval        | 统计信息打印间隔，单位毫秒            | 1000                                                         |

#### Rest 配置

| 配置项            | 描述               | 默认值  |
| ----------------- | ------------------ | ------- |
| restIp            | rest 绑定的 ip     | 0.0.0.0 |
| restPort          | rest 绑定的端口    | 6666    |
| enableRestService | 是否启用 rest 服务 | true    |

#### 元数据配置

| 配置项                    | 描述                                                         | 默认值                |
| ------------------------- | ------------------------------------------------------------ | --------------------- |
| metaStorage               | 元数据存储类型，可选zookeeper, file, etcd 三种               | file                  |
| fileDataDir               | 如果使用 file 作为元数据存储后端，需要提供                   | meta                  |
| zookeeperConnectionString | 如果使用 zookeeper 作为元数据存储后端，需要提供              | 127.0.0.1:2181        |
| etcdEndpoints             | 如果使用 etcd 作为元数据存储后端，需要提供，如果有多个 etcd 实例，以逗号分隔 | http://localhost:2379 |

## 访问

### RESTful 接口

启动完成后，可以便捷地使用 RESTful 接口向 IginX 中写入并查询数据。

创建文件 insert.json，并向其中添加如下的内容：

```json
[
  {
    "name": "archive_file_tracked",
    "datapoints": [
        [1359788400000, 123.3],
        [1359788300000, 13.2 ],
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

使用如下的命令即可向数据库中插入数据：

```shell
$ curl -XPOST -H'Content-Type: application/json' -d @insert.json http://127.0.0.1:6666/api/v1/datapoints
```

在插入数据后，还可以使用 RESTful 接口查询刚刚写入的数据。

创建文件 query.json，并向其中写入如下的数据：

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

使用如下的命令查询数据：

```shell
$ curl -XPOST -H'Content-Type: application/json' -d @query.json http://127.0.0.1:6666/api/v1/datapoints/query
```

命令会返回刚刚插入的数据点信息：

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

更多接口可以参考 [IginX 官方手册](https://github.com/thulab/IginX/blob/main/docs/pdf/userManualC.pdf) 。

### RPC 接口

除了 RESTful 接口外，IginX 还提供了 RPC
的数据访问接口，具体接口参考 [IginX 官方手册](https://github.com/thulab/IginX/blob/main/docs/pdf/userManualC.pdf)，同时 IginX
还提供了部分[官方 example](https://github.com/thulab/IginX/tree/main/example/src/main/java/cn/edu/tsinghua/iginx/session)，展示了
RPC 接口最常见的用法。

下面是一个简短的使用教程。

由于目前 IginX 0.2 版本还未发布到 maven 中央仓库，因此如需使用的话，需要手动安装到本地的 maven 仓库。具体安装方式如下：

```shell
# 下载 iginx 0.2 rc 版本源码包
$ wget https://github.com/thulab/IginX/archive/refs/tags/rc/v0.2.0.tar.gz 
# 解压源码包
$ tar -zxvf v0.2.0.tar.gz
# 进入项目主目录
$ cd IginX-rc-v0.2.0
# 安装到本地 maven 仓库
$ mvn clean install -DskipTests
```

具体在使用时，只需要在相应的项目的 pom 文件中引入如下的依赖：

```xml
<dependency>
  	<groupId>cn.edu.tsinghua</groupId>
  	<artifactId>iginx-core</artifactId>
  	<version>0.1.0-SNAPSHOT</version>
</dependency>
```

在访问 iginx 之前，首先需要创建 session，并尝试连接。Session 构造器有 4 个参数，分别是要连接的 IginX 的 ip，port，以及用于 IginX 认证的用户名和密码。目前的权鉴系统还在编写中，因此访问后端
IginX 的账户名和密码直接填写 root 即可：

```Java
Session session = new Session("127.0.0.1", 6888, "root", "root");
session.openSession();
```

随后可以尝试向 IginX 中插入数据。由于 IginX 支持在数据首次写入时创建时间序列，因此并不需要提前调用相关的序列创建接口。IginX 提供了行式和列式的数据写入接口，以下是列式数据写入接口的使用样例：

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
            Object[] values = new Object[size];
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

在完成数据写入后，可以使用数据查询接口查询刚刚写入的数据：

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

还可以使用降采样聚合查询接口来查询数据的区间统计值：

```java
private static void downsampleQuery(Session session) throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add("sg.d1.s1");
        paths.add("sg.d2.s2");

        long startTime = 100L;
        long endTime = 1101L;

        // MAX
        SessionQueryDataSet dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.MAX, 100);
        dataSet.print();

        // MIN
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.MIN, ROW_INTERVAL * 100);
        dataSet.print();

        // FIRST
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

最终使用完 session 后需要手动关闭，释放连接：

```shell
session.closeSession();
```

完整版使用代码可以参考：https://github.com/thulab/IginX/blob/main/example/src/main/java/cn/edu/tsinghua/iginx/session/IoTDBSessionExample.java

## 基于MAVEN引用IginX类库

### 使用POM

    <repositories>
            <repository>
                <id>github-release-repo</id>
                <name>The Maven Repository on Github</name>
                <url>https://thulab.github.io/IginX/maven-repo/</url>
            </repository>
    </repositories>
    <dependencies>
        <dependency>
            <groupId>cn.edu.tsinghua</groupId>
            <artifactId>iginx-session</artifactId>
            <version>0.3.0</version>
        </dependency>
    </dependencies>