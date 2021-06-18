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
package cn.edu.tsinghua.iginx.metadata;

import cn.edu.tsinghua.iginx.conf.Constants;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TagTools {

    private static final Logger logger = LoggerFactory.getLogger(TagTools.class);

    public boolean hasTags(String path) {
        return path.contains(Constants.TAG_PREFIX);
    }

    public static Pair<String, Map<String, String>> splitTags(String pathWithTags) {
        int splitIndex = pathWithTags.indexOf(Constants.TAG_PREFIX);
        if (splitIndex == -1) { // 没有 tag
            return new Pair<>(pathWithTags, Collections.emptyMap());
        }
        String path = pathWithTags.substring(0, splitIndex - 1);
        List<String> tagKList = new ArrayList<>();
        List<String> tagVList = new ArrayList<>();

        String[] tagKVs = pathWithTags.substring(splitIndex).split("\\.");
        for (String tagKOrV: tagKVs) {
            if (tagKOrV.startsWith(Constants.TAG_PREFIX)) {
                tagKList.add(tagKOrV.substring(Constants.TAG_PREFIX.length()));
            } else if (tagKOrV.startsWith(Constants.VALUE_PREFIX)) {
                tagVList.add(tagKOrV.substring(Constants.VALUE_PREFIX.length()));
            } else {
                throw new IllegalArgumentException("unexpected path with tags: " + pathWithTags);
            }
        }
        if (tagKList.size() != tagVList.size()) {
            throw new IllegalArgumentException("unexpected path with tags: " + pathWithTags);
        }
        Map<String, String> tags = new HashMap<>();
        for (int i = 0; i < tagKList.size(); i++) {
            tags.put(tagKList.get(i), tagVList.get(i));
        }
        return new Pair<>(path, tags);
    }

    public static List<Pair<String, Map<String, String>>> splitTags(List<String> pathsWithTags) {
        if (pathsWithTags == null) {
            return Collections.emptyList();
        }
        return pathsWithTags.stream().map(TagTools::splitTags).collect(Collectors.toList());
    }

    public static List<String> concatTags(List<String> paths, List<Map<String, String>> tagsList) {
        if (tagsList == null || tagsList.size() == 0) {
            return paths;
        }
        List<String> pathsWithTags = new ArrayList<>();
        for (int i = 0; i < paths.size(); i++) {
            String path = paths.get(i);
            Map<String, String> tags = tagsList.get(i);
            if (tags == null || tags.size() == 0) {
                pathsWithTags.add(path);
                continue;
            }

            List<Map.Entry<String, String>> tagList = tags.entrySet().stream().sorted(Map.Entry.comparingByKey()).collect(Collectors.toList());
            StringBuilder tagKBuilder = new StringBuilder();
            StringBuilder tagVBuilder = new StringBuilder();

            for (Map.Entry<String, String> tag : tagList) {
                tagKBuilder.append(Constants.DOT);
                tagKBuilder.append(Constants.TAG_PREFIX);
                tagKBuilder.append(tag.getKey());

                tagVBuilder.append(Constants.DOT);
                tagVBuilder.append(Constants.VALUE_PREFIX);
                tagVBuilder.append(tag.getValue());
            }
            pathsWithTags.add(path + tagKBuilder.toString() + tagVBuilder.toString());
        }
        return pathsWithTags;
    }

    public static String toString(String path, Map<String, String> tags) {
        if (tags == null || tags.isEmpty()) {
            return path;
        }
        StringBuilder builder = new StringBuilder(path);
        List<Map.Entry<String, String>> tagList = tags.entrySet().stream().sorted(Map.Entry.comparingByKey()).collect(Collectors.toList());
        for (int i = 0; i < tagList.size(); i++) {
            Map.Entry<String, String> tag = tagList.get(i);
            if (i == 0) {
                builder.append("{");
            } else {
                builder.append(", ");
            }
            builder.append(tag.getKey());
            builder.append("=");
            builder.append(tag.getValue());
        }
        builder.append('}');
        return builder.toString();
    }

    public static void main(String[] args) {
        List<String> paths = Arrays.asList("cpu.usage", "memory.usage", "java");
        List<Map<String, String>> tagsList = new ArrayList<>();

        Map<String, String> tags1 = new HashMap<>();
        tags1.put("host", "1");
        tagsList.add(tags1);

        Map<String, String> tags2 = new HashMap<>();
        tags2.put("host", "1");
        tags2.put("type", "ddr4");
        tags2.put("producer", "Samsung");
        tagsList.add(tags2);

        Map<String, String> tags3 = new HashMap<>();
        tagsList.add(tags3);

        List<String> pathsWithTags = concatTags(paths, tagsList);
        for (String pathWithTags: pathsWithTags) {
            System.out.println(pathWithTags);
            Pair<String, Map<String, String>> pathAndTags = splitTags(pathWithTags);
            System.out.println(pathAndTags.k);
            System.out.println(pathAndTags.v);
            System.out.println(toString(pathAndTags.k, pathAndTags.v));
        }
        System.out.println();
    }

}
