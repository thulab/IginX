# IginX 安装配置手册

[TOC]

IginX 是清华大学大数据系统软件国家工程实验室，为满足工业互联网场景推出的新一代高可扩展时序数据库分布式中间件，目前支持 IoTDB，InfluxDB 作为数据后端。

## 下载安装

IginX 为您提供了两种安装方式，您可以参考下面的建议，任选其中一种：

1. 从官网下载安装包。这是我们推荐使用的安装方式，通过该方式，您将得到一个可以立即使用的、打包好的二进制可执行文件。
2. 使用源码编译。若您需要自行修改代码，可以使用该安装方式。

### 环境准备

#### Java 安装

由于 ZooKeeper、IginX 以及 IoTDB 都是使用 Java 开发的，因此首先需要安装 Java。如果本地已经安装了 JDK>=1.8 的运行环境，**直接跳过此步骤**。

1. 首先访问 [Java官方网站](https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html)下载适用于当前系统的 JDK  包。
2. 安装

```shell
$ cd ~/Downloads
$ tar -zxf jdk-8u181-linux-x64.gz # 解压文件
$ mkdir /opt/jdk
$ mv jdk-1.8.0_181 /opt/jdk/
```

1. 设置路径

编辑 ~/.bashrc 文件，在文件末端加入如下的两行：

```shell
export JAVA_HOME = /usr/jdk/jdk-1.8.0_181
export PATH=$PATH:$JAVA_HOME/bin
```

加载更改后的配置文件：

```shell
$ source ~/.bashrc
```

2. 使用 java -version 判断 JDK 是否安装成功

```shell
$ java -version
java version "1.8.0_181"
Java(TM) SE Runtime Environment (build 1.8.0_181-b13)
Java HotSpot(TM) 64-Bit Server VM (build 25.181-b13, mixed mode)
```

如果显示出如上的字样，则表示安装成功。

#### Maven 安装

Maven 是 Java 项目管理和自动构建工具，如果您需要从源码进行编译，还需要安装 Maven >= 3.6 的环境，否则，**直接跳过此步骤**。

在安装 ZooKeeper 之前添加了一个步骤。 在使用之前检查 wget 的安装将节省时间。

1. 访问[官网](http://maven.apache.org/download.cgi)下载并解压 Maven

```shell
$ wget http://mirrors.hust.edu.cn/apache/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz
$ tar -xvf  apache-maven-3.3.9-bin.tar.gz
$ sudo mv -f apache-maven-3.3.9 /usr/local/
```

1. 设置路径

编辑 ~/.bashrc 文件，在文件末端加入如下的两行：

```shell
export MAVEN_HOME=/usr/local/apache-maven-3.3.9
export PATH=${PATH}:${MAVEN_HOME}/bin
```

加载更改后的配置文件：

```shell
$ source ~/.bashrc
```

1. 使用 mvn -v 判断 Maven 是否安装成功

```shell
$ mvn -v
Apache Maven 3.6.1 (d66c9c0b3152b2e69ee9bac180bb8fcc8e6af555; 2019-04-05T03:00:29+08:00)
```

如果显示出如上的字样，则表示安装成功。

#### ZooKeeper 安装

ZooKeeper 是 Apache 推出的开源的分布式应用程序协调服务。如果您需要部署大于一个 IginX 实例，则需要安装 ZooKeeper，否则，**直接跳过此步骤**。

ZooKeeper 是 Apache 推出的开源的分布式应用程序协调服务。具体安装方式如下：

1. 访问[官网](https://zookeeper.apache.org/releases.html)下载并解压 ZooKeeper

```shell
$ cd ~
$ wget https://mirrors.bfsu.edu.cn/apache/zookeeper/zookeeper-3.7.0/apache-zookeeper-3.7.0-bin.tar.gz
$ tar -zxvf apache-zookeeper-3.7.0-bin.tar.gz
```

1. 修改 ZooKeeper 默认配置文件

```shell
$ cd apache-zookeeper-3.7.0-bin/
$ mkdir data
$ cp conf/zoo_sample.cfg conf/zoo.cfg
```

然后编辑 conf/zoo.cfg 文件，将

```shell
dataDir=/tmp/zookeeper
```

修改为

```shell
dataDir=data
```

#### IoTDB 安装

IoTDB 是 Apache 推出的时序数据库，具体安装方式如下：

```shell
$ cd ~
$ wget https://mirrors.bfsu.edu.cn/apache/iotdb/0.12.0/apache-iotdb-0.12.0-server-bin.zip
$ unzip apache-iotdb-0.12.0-server-bin.zip
```

### 下载二进制可执行文件

直接访问 [IginX 项目](https://github.com/thulab/IginX/)下载 [IginX 项目发布包](https://github.com/thulab/IginX/releases/download/rc/v0.2.0/IginX-release-v0.2.0-bin.tar.gz)
即可

```shell
$ cd ~
$ wget https://github.com/thulab/IginX/releases/download/release%2Fv0.4.0/IginX-release-v0.4.0-bin.zip
$ unzip IginX-release-v0.4.0-bin.zip
```

### 使用源码编译

拉取最新开发版本，并进行本地构建

```shell
$ cd ~
$ git clone git@github.com:thulab/IginX.git
$ cd IginX
$ mvn clean install -Dmaven.test.skip=true
```

## 配置启动

### 单节点

单节点配置启动指的是单个 IginX 实例，底层为单个 IoTDB 实例的启动

#### 启动 IoTDB

首先需要启动 IoTDB。

```shell
$ cd ~
$ cd apache-iotdb-0.12.0-server-bin/
$ ./sbin/start-server.sh
```

显示出如下字样，表示 IoTDB 启动成功：

```shell
2021-05-27 08:21:07,440 [main] INFO  o.a.i.d.s.t.ThriftService:125 - IoTDB: start RPC ServerService successfully, listening on ip 0.0.0.0 port 6667
2021-05-27 08:21:07,440 [main] INFO  o.a.i.db.service.IoTDB:129 - IoTDB is set up, now may some sgs are not ready, please wait several seconds...
2021-05-27 08:21:07,448 [main] INFO  o.a.i.d.s.UpgradeSevice:109 - finish counting upgrading files, total num:0
2021-05-27 08:21:07,449 [main] INFO  o.a.i.d.s.UpgradeSevice:74 - Waiting for upgrade task pool to shut down
2021-05-27 08:21:07,449 [main] INFO  o.a.i.d.s.UpgradeSevice:76 - Upgrade service stopped
2021-05-27 08:21:07,449 [main] INFO  o.a.i.db.service.IoTDB:146 - Congratulation, IoTDB is set up successfully. Now, enjoy yourself!
2021-05-27 08:21:07,450 [main] INFO  o.a.i.db.service.IoTDB:93 - IoTDB has started.
```

#### 启动 ZooKeeper

如果您采取的是 0.4.0 的二进制安装包，或者在配置文件中指定 ZooKeeper 为元数据管理存储后端，需要启动ZooKeeper。否则，**直接跳过此步骤**

```shell
$ cd ~
$ cd apache-zookeeper-3.7.0-bin/
$ ./bin/zkServer.sh start
```

显示出如下字样，表示 ZooKeeper 启动成功

```shell
ZooKeeper JMX enabled by default
Using config: /home/root/apache-zookeeper-3.7.0-bin/bin/../conf/zoo.cfg
Starting zookeeper ... STARTED
```

#### 启动 IginX

使用发布包启动

```shell
$ cd ~
$ cd IginX-release-v0.4.0-bin
$ chmod +x startIginX.sh # 为启动脚本添加启动权限
$ ./startIginX.sh
```

使用源码启动

```shell
$ cd ~
$ cd Iginx
$ chmod +x startIginX.sh # 为启动脚本添加启动权限
$ ./startIginX.sh
```

### 集群

IginX 元数据存储管理现在有 ZooKeeper 存储、写本地文件两种方式。

当部署场景为多 IginX 实例时，必须采用 ZooKeeper 存储，部署单个 IginX 实例，且采用源码编译安装方式时，两者均可选择，只需要更改对应的配置文件即可。

这里以两个 IoTDB 实例、两个 IginX 实例为例

#### 启动多个 IoTDB 实例

这里以单机启动两个端口分别为 6667 和 7667 的实例为例

```shell
$ cd ~
$ cd apache-iotdb-0.12.0-server-bin/
$ ./sbin/start-server.sh # 启动实例一 127.0.0.1: 6667
```

修改配置文件 IoTDB_HOME/conf/iotdb-engine.properties

```shell
rpc_port=7667
```

启动第二个实例

```shell
$ ./sbin/start-server.sh # 启动实例二 127.0.0.1: 7667
```

#### 启动多个 IginX 实例

修改 IginX_HOME/conf/config. Properties

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

启动第一个 IginX 实例

```shell
$ cd ~
$ cd Iginx
$ chmod +x sbin/start_iginx.sh # 为启动脚本添加启动权限
$ ./sbin/start_iginx.sh
```

修改 conf/config. Properties

```shell
# iginx 绑定的端口
port=7888
# rest 绑定的端口
restPort=7666
```

启动第二个 IginX 实例

```shell
$ ./sbin/start_iginx.sh
```

### 配置项

为方便 IginX 的配置与管理，IginX 为用户提供多项可选配置。IginX 配置文件位于 IginX 安装目录 `$IginX_HOME/conf` 文件夹下的 `config.properties` 中，主要包括
IginX、Rest、元数据管理三方面配置。

#### IginX 配置

| 配置项                       | 描述                                  | 默认值                                                       |
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
$ wget https://github.com/thulab/IginX/archive/refs/tags/rc/v0.4.0.tar.gz 
# 解压源码包
$ tar -zxvf v0.4.0.tar.gz
# 进入项目主目录
$ cd IginX-rc-v0.4.0
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
