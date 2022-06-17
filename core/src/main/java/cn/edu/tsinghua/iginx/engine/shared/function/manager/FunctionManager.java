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
package cn.edu.tsinghua.iginx.engine.shared.function.manager;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.shared.function.Function;
import cn.edu.tsinghua.iginx.engine.shared.function.system.*;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.python.PyUDAF;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.python.PyUDSF;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.python.PyUDTF;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.TransformTaskMeta;
import cn.edu.tsinghua.iginx.thrift.UDFType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pemja.core.PythonInterpreter;
import pemja.core.PythonInterpreterConfig;

import java.io.File;
import java.util.*;

public class FunctionManager {

    private final Map<String, Function> functions;

    private final static IMetaManager metaManager = DefaultMetaManager.getInstance();

    private final static Config config = ConfigDescriptor.getInstance().getConfig();

    private static final Logger logger = LoggerFactory.getLogger(FunctionManager.class);

    private static final String PY_SUFFIX = ".py";

    private static final String PATH = String.join(File.separator, System.getProperty("user.dir"), "python_scripts");

    private FunctionManager() {
        this.functions = new HashMap<>();
        this.initSystemFunctions();
        if (config.isNeedInitBasicUDFFunctions()) {
            this.initBasicUDFFunctions();
        }
    }

    public static FunctionManager getInstance() {
        return FunctionManagerHolder.INSTANCE;
    }

    private void initSystemFunctions() {
        registerFunction(Avg.getInstance());
        registerFunction(Count.getInstance());
        registerFunction(FirstValue.getInstance());
        registerFunction(LastValue.getInstance());
        registerFunction(First.getInstance());
        registerFunction(Last.getInstance());
        registerFunction(Max.getInstance());
        registerFunction(Min.getInstance());
        registerFunction(Sum.getInstance());
    }

    private void initBasicUDFFunctions() {
        List<TransformTaskMeta> metaList = new ArrayList<>();
        String[] udfList = config.getUdfList().split(",");
        for (String udf: udfList) {
            String[] udfInfo = udf.split("#");
            if (udfInfo.length != 4) {
                logger.error("udf info len must be 4.");
                continue;
            }
            UDFType udfType;
            switch (udfInfo[3].toLowerCase().trim()) {
                case "udaf":
                    udfType = UDFType.UDAF;
                    break;
                case "udtf":
                    udfType = UDFType.UDTF;
                    break;
                case "udsf":
                    udfType = UDFType.UDSF;
                    break;
                case "transform":
                    udfType = UDFType.TRANSFORM;
                    break;
                default:
                    logger.error("unknown udf type: " + udfInfo[3]);
                    continue;
            }
            metaList.add(new TransformTaskMeta(udfInfo[0], udfInfo[1], udfInfo[2], config.getIp(), udfType));
        }
//        List<TransformTaskMeta> metaList = Arrays.asList(
//            new TransformTaskMeta("udf_min", "UDFMin", "udf_min.py", config.getIp(), UDFType.UDAF),
//            new TransformTaskMeta("udf_max", "UDFMax", "udf_max.py", config.getIp(), UDFType.UDAF),
//            new TransformTaskMeta("udf_sum", "UDFSum", "udf_sum.py", config.getIp(), UDFType.UDAF),
//            new TransformTaskMeta("udf_avg", "UDFAvg", "udf_avg.py", config.getIp(), UDFType.UDAF),
//            new TransformTaskMeta("udf_count", "UDFCount", "udf_count.py", config.getIp(), UDFType.UDAF)
//        );

        for (TransformTaskMeta meta : metaList) {
            TransformTaskMeta taskMeta = metaManager.getTransformTask(meta.getName());
            if (taskMeta == null) {
                metaManager.addTransformTask(meta);
            }
            loadUDF(meta.getName());
        }
    }

    public void registerFunction(Function function) {
        if (functions.containsKey(function.getIdentifier())) {
            return;
        }
        functions.put(function.getIdentifier(), function);
    }

    public Collection<Function> getFunctions() {
        return functions.values();
    }

    public Function getFunction(String identifier) {
        if (functions.containsKey(identifier)) {
            return functions.get(identifier);
        }
        return loadUDF(identifier);
    }

    private Function loadUDF(String identifier) {
        // load the udf & put it in cache.
        TransformTaskMeta taskMeta = metaManager.getTransformTask(identifier);
        if (taskMeta == null) {
            throw new IllegalArgumentException(String.format("UDF %s not registered", identifier));
        }
        if (!taskMeta.getIp().equals(config.getIp())) {
            throw new IllegalArgumentException(String.format("UDF %s registered in node ip=%s", identifier, taskMeta.getIp()));
        }

        String pythonCMD = config.getPythonCMD();
        PythonInterpreterConfig config = PythonInterpreterConfig
            .newBuilder()
            .setPythonExec(pythonCMD)
            .addPythonPaths(PATH)
            .build();

        PythonInterpreter interpreter = new PythonInterpreter(config);
        String fileName = taskMeta.getFileName();
        String moduleName = fileName.substring(0, fileName.indexOf(PY_SUFFIX));
        String className = taskMeta.getClassName();

        // init the python udf
        interpreter.exec(String.format("import %s", moduleName));
        interpreter.exec(String.format("t = %s.%s()", moduleName, className));

        if (taskMeta.getType().equals(UDFType.UDAF)) {
            PyUDAF udaf = new PyUDAF(interpreter, identifier);
            functions.put(identifier, udaf);
            return udaf;
        } else if (taskMeta.getType().equals(UDFType.UDTF)) {
            PyUDTF udtf = new PyUDTF(interpreter, identifier);
            functions.put(identifier, udtf);
            return udtf;
        } else if (taskMeta.getType().equals(UDFType.UDSF)) {
            PyUDSF udsf = new PyUDSF(interpreter, identifier);
            functions.put(identifier, udsf);
            return udsf;
        } else {
            throw new IllegalArgumentException(String.format("UDF %s registered in type %s", identifier, taskMeta.getType()));
        }
    }

    public boolean isUDTF(String identifier) {
        Function function = getFunction(identifier);
        return function.getIdentifier().equals("py_udtf");
    }

    public boolean isUDAF(String identifier) {
        Function function = getFunction(identifier);

        return function.getIdentifier().equals("py_udaf");
    }

    public boolean isUDSF(String identifier) {
        Function function = getFunction(identifier);
        return function.getIdentifier().equals("py_udsf");
    }

    public boolean hasFunction(String identifier) {
        return functions.containsKey(identifier);
    }

    private static class FunctionManagerHolder {

        private static final FunctionManager INSTANCE = new FunctionManager();

        private FunctionManagerHolder() {
        }

    }

}
