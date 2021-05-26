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
package cn.edu.tsinghua.iginx.metadata.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.nio.charset.StandardCharsets;

public class JsonUtils {

	private static final Gson gson = new GsonBuilder()
			.create();

	public static byte[] toJson(Object o) {
		return gson.toJson(o).getBytes(StandardCharsets.UTF_8);
	}

	public static <T> T fromJson(byte[] data, Class<T> clazz) {
		return gson.fromJson(new String(data), clazz);
	}

	public static Gson getGson() {
		return gson;
	}
}
