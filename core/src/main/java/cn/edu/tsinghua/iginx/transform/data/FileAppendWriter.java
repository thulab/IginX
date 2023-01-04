package cn.edu.tsinghua.iginx.transform.data;

import cn.edu.tsinghua.iginx.constant.GlobalConstant;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class FileAppendWriter extends ExportWriter {

    private final String fileName;

    private boolean hasWriteHeader;

    private final static Logger logger = LoggerFactory.getLogger(FileAppendWriter.class);

    public FileAppendWriter(String fileName) {
        this.fileName = fileName;
        this.hasWriteHeader = false;
        File file = new File(fileName);
        createFileIfNotExist(file);
    }

    @Override
    public void write(BatchData batchData) {
        if (!hasWriteHeader) {
            Header header = batchData.getHeader();

            List<String> headerList = new ArrayList<>();
            if (header.hasKey()) {
                headerList.add(GlobalConstant.KEY_NAME);
            }
            header.getFields().forEach(field -> headerList.add(field.getFullName()));
            writeFile(fileName, String.join(",", headerList) + "\n");
            hasWriteHeader = true;
        }
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
