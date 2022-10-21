package cn.edu.tsinghua.iginx.migration.recover;

import static cn.edu.tsinghua.iginx.migration.recover.MigrationLogger.MIGRATION_EXECUTE_TASK_END;
import static cn.edu.tsinghua.iginx.migration.recover.MigrationLogger.MIGRATION_EXECUTE_TASK_START;
import static cn.edu.tsinghua.iginx.migration.recover.MigrationLogger.MIGRATION_FINISHED;
import static cn.edu.tsinghua.iginx.migration.recover.MigrationLogger.MIGRATION_LOG_NAME;
import static cn.edu.tsinghua.iginx.migration.recover.MigrationLogger.SOURCE_NAME;

import cn.edu.tsinghua.iginx.migration.MigrationTask;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MigrationLoggerAnalyzer {

  private File logFile;

  private List<MigrationTask> migrationTasks = new ArrayList<>();
  private List<MigrationExecuteTask> migrationExecuteTasks = new ArrayList<>();
  private boolean isStartMigration;
  private boolean isLastMigrationExecuteTaskFinished;
  private boolean isMigrationFinished;

  public MigrationLoggerAnalyzer() {
    this.logFile = new File(MIGRATION_LOG_NAME);
  }

  public void analyze() throws IOException {
    if (logFile.exists()) {
      String currLine;
      try (BufferedReader bufferedReader = new BufferedReader(new FileReader(logFile))) {
        while ((currLine = bufferedReader.readLine()) != null) {
          switch (currLine) {
            case "\n":
              break;
            case SOURCE_NAME:
              isStartMigration = true;
              break;
            case MIGRATION_EXECUTE_TASK_START:
              isLastMigrationExecuteTaskFinished = false;
              currLine = bufferedReader.readLine();
              migrationExecuteTasks.add(MigrationExecuteTask.fromString(currLine));
              break;
            case MIGRATION_EXECUTE_TASK_END:
              isLastMigrationExecuteTaskFinished = true;
              break;
            case MIGRATION_FINISHED:
              isMigrationFinished = true;
              break;
            default:
              migrationTasks.add(MigrationTask.fromString(currLine));
              break;
          }
        }
      }
    }
  }

  public File getLogFile() {
    return logFile;
  }

  public List<MigrationTask> getMigrationTasks() {
    return migrationTasks;
  }

  public List<MigrationExecuteTask> getMigrationExecuteTasks() {
    return migrationExecuteTasks;
  }

  public boolean isStartMigration() {
    return isStartMigration;
  }

  public boolean isLastMigrationExecuteTaskFinished() {
    return isLastMigrationExecuteTaskFinished;
  }

  public MigrationExecuteTask getLastMigrationExecuteTask() {
    if (migrationExecuteTasks.size() > 0) {
      return migrationExecuteTasks.get(migrationExecuteTasks.size() - 1);
    }
    return null;
  }

  public boolean isMigrationFinished() {
    return isMigrationFinished;
  }
}