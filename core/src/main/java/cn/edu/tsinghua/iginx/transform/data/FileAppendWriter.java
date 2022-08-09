package cn.edu.tsinghua.iginx.transform.data;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class FileAppendWriter extends ExportWriter {

    private final String fileName;

    private final List<String> exportNameList;

    private final static Logger logger = LoggerFactory.getLogger(FileAppendWriter.class);

    public FileAppendWriter(String fileName, List<String> exportNameList) {
        this.fileName = fileName;
        this.exportNameList = exportNameList;
        File file = new File(fileName);
        createFileIfNotExist(file);
    }

    @Override
    public void write(BatchData batchData) {
        for (Row row : batchData.getRowList()) {
            writeFile(fileName, row.toCSVTypeString() + "\n");
        }
    }

    private void createFileIfNotExist(File file) {
        if (!file.exists()) {
            logger.info("File not exists, create it...");
            // get and create parent dir
            if (!file.getParentFile().exists()) {
                System.out.println("Parent dir not exists, create it...");
                file.getParentFile().mkdirs();
            }
            try {
                // create file
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (exportNameList != null && !exportNameList.isEmpty()) {
            writeFile(fileName, String.join(",", exportNameList) + "\n");
        }
    }

    private void writeFile(String fileName, String content) {
        try {
            File file = new File(fileName);

            try (FileWriter writer = new FileWriter(file, true);
                 BufferedWriter out = new BufferedWriter(writer)
            ) {
                out.write(content);
                out.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
