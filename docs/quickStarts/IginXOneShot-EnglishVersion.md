# IginX Installation and Use Manual (One-Click Start)

[TOC]

IginX is is a new-generation highly scalable time series database distributed middleware, designed to meet industrial Internet scenarios. It was launched by Tsinghua University's National Engineering Laboratory of Big Data System Software. It currently supports IoTDB，InfluxDB as data backends.

## Installation

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

### IginX installation

IginX is the main part of the system, and the installation package can be downloaded and started with one click:

```shell
$ cd ~
$ wget https://github.com/thulab/IginX/releases/download/release%2Fv0.2.0/IginX-release-v0.2.0-bin-3in1.zip
$ unzip IginX-release-v0.2.0-bin-3in1.zip
````

## Launch

```shell
$ cd ~
$ cd IginX-release-v0.2.0-bin-3in1
$ chmod +x ./startAllOnSingleMachine.sh
$ ./startAllOnSingleMachine.sh
````

The following display of words means the IginX installation was successful：

```shell
ZooKeeper is started!
IoTDB is started!
IginX is started!
=========================================
You can now test IginX. Have fun!~
=========================================
````

## Using IginX

### RESTful interface

After the startup is complete, you can easily use the RESTful interface to write and query data to IginX.

Create the file insert.json and add the following to it:

````json
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
````

Use the following command to insert data into the database:

```shell
$ curl -XPOST -H'Content-Type: application/json' -d @insert.json http://127.0.0.1:6666/api/v1/datapoints
````

After inserting data, you can also query the data just written using the RESTful interface.

Create a file query.json and write the following data to it:

````json
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
````

Use the following command to query the data:

```shell
$ curl -XPOST -H'Content-Type: application/json' -d @query.json http://127.0.0.1:6666/api/v1/datapoints/query
````

The command will return information about the data point just inserted:

````json
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
````

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