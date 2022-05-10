package cn.edu.tsinghua.iginx.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

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

    public JobFromYAML getJobFromYAML() {
        try {
            return yaml.load(new FileInputStream(file));
        } catch (FileNotFoundException e) {
            logger.error(String.format("Fail to find file, path=%s", file.getAbsolutePath()));
            return null;
        }
    }

}
