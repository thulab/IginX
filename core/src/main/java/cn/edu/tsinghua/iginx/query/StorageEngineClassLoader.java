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
package cn.edu.tsinghua.iginx.query;

import cn.edu.tsinghua.iginx.conf.Constants;
import cn.edu.tsinghua.iginx.utils.EnvUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class StorageEngineClassLoader extends ClassLoader {

    private final String path;

    private final Map<String, String> nameToJar;

    public StorageEngineClassLoader(String path) throws IOException {
        this.path = EnvUtils.loadEnv(Constants.DRIVER, Constants.DRIVER_DIR) + path;
        this.nameToJar = new HashMap<>();
        preloadClassNames();
    }

    private void preloadClassNames() throws IOException {
        List<File> jars = new ArrayList<>();
        File[] files = new File(path).listFiles();
        if (files == null) {
            return;
        }
        for (File f : files) {
            if (f.isFile() && f.getName().endsWith(".jar"))
                jars.add(f);
        }
        for (File jar: jars) {
            Enumeration<JarEntry> entries = new JarFile(jar).entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                String name = entry.getName();
                if (name.endsWith(".class")) {
                    String clss = name.replace(".class", "").replaceAll("/", ".");
                    if (this.findLoadedClass(clss) != null) {
                        continue;
                    }
                    nameToJar.put(clss, jar.getAbsolutePath());
                }
            }
        }
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        byte[] result = getClassFromJars(name);
        if (result == null) {
            throw new ClassNotFoundException("unable to find class: " + name);
        } else {
            return defineClass(name, result, 0, result.length);
        }
    }

    private byte[] getClassFromJars(String name) {
        String jarPath = nameToJar.get(name);
        if (jarPath == null) {
            return null;
        }
        try {
            JarFile jar = new JarFile(jarPath);
            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                String entryName = entry.getName();
                if (entryName.endsWith(".class")) {
                    String entryClass = entryName.replace(".class", "").replaceAll("/", ".");
                    if (entryClass.equals(name)) {
                        InputStream input = jar.getInputStream(entry);
                        ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        int bufferSize = 1024;
                        byte[] buffer = new byte[bufferSize];
                        int bytesNumRead;
                        while ((bytesNumRead = input.read(buffer)) != -1) {
                            baos.write(buffer, 0, bytesNumRead);
                        }
                        byte[] cc = baos.toByteArray();
                        input.close();
                        return cc;
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
