package cn.edu.tsinghua.iginx.utils;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FileReader {
    static String path;

    static private File file;

    private final static Logger logger = LoggerFactory.getLogger(FileReader.class);

    public FileReader(String path) throws FileNotFoundException {
        path = path;
        file = new File(path);
    }

    static public String convertToString(String filePath) {
        String conf = null;
        InputStream in = null;
        try {
            in = new BufferedInputStream(Files.newInputStream(Paths.get(filePath)));
            conf = IOUtils.toString(in, String.valueOf(StandardCharsets.UTF_8));
        } catch (IOException e) {
            logger.error(String.format("Fail to find file, path=%s", filePath));
        } finally {
            try {
                if(in!=null) in.close();
            } catch (IOException e) {
                logger.error("Fail to close the file, path={}", filePath);
            }
        }
        return conf;
    }
}
