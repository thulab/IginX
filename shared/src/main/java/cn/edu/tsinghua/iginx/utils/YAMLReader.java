package cn.edu.tsinghua.iginx.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.apache.commons.io.IOUtils;


import java.io.*;
import java.nio.charset.StandardCharsets;

public class YAMLReader {

    private final String path;
    
    private final Yaml yaml;

    private final File file;

    private final static Logger logger = LoggerFactory.getLogger(YAMLReader.class);

    public YAMLReader(String path) throws FileNotFoundException {
        this.path = path;
        this.yaml = new Yaml(new Constructor(JobFromYAML.class));
        this.file = new File(path);
    }

    public String normalize(String conf) {
        String taskType = "(?i)taskType";
        String dataFlowType = "(?i)dataFlowType";
        String timeout = "(?i)timeout";
        String pyTaskName = "(?i)pyTaskName";
        String sqlList = "(?i)sqlList";

        conf = conf.replaceAll(taskType, "taskType");
        conf = conf.replaceAll(dataFlowType, "dataFlowType");
        conf = conf.replaceAll(timeout, "timeout");
        conf = conf.replaceAll(pyTaskName, "pyTaskName");
        conf = conf.replaceAll(sqlList, "sqlList");

        return conf;
    }

    public String convertToString(String filePath) {
        String conf = null;
        InputStream in = null;
        try {
            in = new BufferedInputStream(new FileInputStream(filePath));
            conf = IOUtils.toString(in, String.valueOf(StandardCharsets.UTF_8));
            conf = normalize(conf);
        } catch (IOException e) {
            logger.error(String.format("Fail to find file, path=%s", filePath));
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                logger.error("Fail to close the file, path=%s", filePath);
            }
        }
        return conf;
    }

    public JobFromYAML getJobFromYAML() {
        String yamlFile = convertToString(path);
        InputStream result = new ByteArrayInputStream(yamlFile.getBytes(StandardCharsets.UTF_8));
        return yaml.load(result);
    }

}
