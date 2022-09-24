package cn.edu.tsinghua.iginx.transform;

import cn.edu.tsinghua.iginx.transform.exception.TransformException;
import cn.edu.tsinghua.iginx.utils.JobFromYAML;
import cn.edu.tsinghua.iginx.utils.TaskFromYAML;
import cn.edu.tsinghua.iginx.utils.YAMLReader;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.List;

public class yamlReadTest {

    final String filePath = "../example/src/main/resources/TransformJobExample.yaml";

    private final static Logger logger = LoggerFactory.getLogger(YAMLReader.class);

    @Test
    public void test() throws FileNotFoundException {
        try {
            YAMLReader yamlReader = new YAMLReader(filePath);
            JobFromYAML jobFromYAML = yamlReader.getJobFromYAML();
            List<TaskFromYAML> tasks = jobFromYAML.getTaskList();
            System.out.println("exportType: " + jobFromYAML.getExportType());
            System.out.println("exportFile: " + jobFromYAML.getExportFile());
            for(String job : jobFromYAML.getExportNameList())
                System.out.println("ExportNameList: " + job);

            System.out.println("\n");

            for(TaskFromYAML task : tasks) {
                System.out.println("TaskType: " + task.getTaskType());
                System.out.println("dataFlowType: " + task.getDataFlowType());
                System.out.println("timeout: " + task.getTimeout());
                System.out.println("pyTaskName: " + task.getPyTaskName());
                for(String sql : task.getSqlList())
                    System.out.println("SqlList: " + sql);
                System.out.println("--------------------------------------------");
            }

        }catch (FileNotFoundException e) {
            logger.error("Fail to close the file, path=%s", filePath);
        }
    }

}
