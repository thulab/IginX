/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.utils;

public class SnowFlakeUtils {

	// 起始的时间戳
	private final static long START_STAMP = 0L;
	// 每一部分占用的位数，就三个
	private final static long SEQUENCE_BIT = 12;// 序列号占用的位数
	private final static long MACHINE_BIT = 5; // 机器标识占用的位数
	private final static long DATACENTER_BIT = 5;// 数据中心占用的位数
	// 每一部分最大值
	private final static long MAX_DATACENTER_NUM = ~(-1L << DATACENTER_BIT);
	private final static long MAX_MACHINE_NUM = ~(-1L << MACHINE_BIT);
	private final static long MAX_SEQUENCE = ~(-1L << SEQUENCE_BIT);
	// 每一部分向左的位移
	private final static long MACHINE_LEFT = SEQUENCE_BIT;
	private final static long DATACENTER_LEFT = SEQUENCE_BIT + MACHINE_BIT;
	private final static long TIMESTAMP_LEFT = DATACENTER_LEFT + DATACENTER_BIT;
	private static SnowFlakeUtils instance;
	private final long datacenterId; // 数据中心

	private final long machineId; // 机器标识

	private long sequence = 0L; // 序列号

	private long lastStamp = -1L;// 上一次时间戳

	public SnowFlakeUtils(long datacenterId, long machineId) {
		if (datacenterId > MAX_DATACENTER_NUM || datacenterId < 0) {
			throw new IllegalArgumentException("datacenterId can't be greater than MAX_DATACENTER_NUM or less than 0");
		}
		if (machineId > MAX_MACHINE_NUM || machineId < 0) {
			throw new IllegalArgumentException("machineId can't be greater than MAX_MACHINE_NUM or less than 0");
		}
		this.datacenterId = datacenterId;
		this.machineId = machineId;
	}

	public static void init(long machineId) {
		if (instance != null) {
			return;
		}
		instance = new SnowFlakeUtils(0, machineId);
	}

	public static SnowFlakeUtils getInstance() {
		return instance;
	}

	//产生下一个ID
	public synchronized long nextId() {
		long currStamp = getNewTimestamp();
		//如果当前时间小于上一次ID生成的时间戳，说明系统时钟回退过  这个时候应当抛出异常
		if (currStamp < lastStamp) {
			throw new RuntimeException("Clock moved backwards.  Refusing to generate id");
		}

		if (currStamp == lastStamp) {
			//if条件里表示当前调用和上一次调用落在了相同毫秒内，只能通过第三部分，序列号自增来判断为唯一，所以+1.
			sequence = (sequence + 1) & MAX_SEQUENCE;
			//同一毫秒的序列数已经达到最大，只能等待下一个毫秒
			if (sequence == 0L) {
				currStamp = getNextMill();
			}
		}
		//时间戳改变，毫秒内序列重置
		else {
			//不同毫秒内，序列号置为0
			//执行到这个分支的前提是currTimestamp > lastTimestamp，说明本次调用跟上次调用对比，已经不再同一个毫秒内了，这个时候序号可以重新回置0了。
			sequence = 0L;
		}

		lastStamp = currStamp;
		//就是用相对毫秒数、机器ID和自增序号拼接
		//移位  并通过  或运算拼到一起组成64位的ID
		return (currStamp - START_STAMP) << TIMESTAMP_LEFT //时间戳部分
				| datacenterId << DATACENTER_LEFT      //数据中心部分
				| machineId << MACHINE_LEFT            //机器标识部分
				| sequence;                            //序列号部分
	}

	private long getNextMill() {
		long mill = getNewTimestamp();
		//使用while循环等待直到下一毫秒。
		while (mill <= lastStamp) {
			mill = getNewTimestamp();
		}
		return mill;
	}

	private long getNewTimestamp() {
		return System.currentTimeMillis();
	}

}