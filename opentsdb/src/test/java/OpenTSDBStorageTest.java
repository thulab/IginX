import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.task.StoragePhysicalTask;
import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawData;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawDataType;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RowDataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.operator.Insert;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.opentsdb.OpenTSDBStorage;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

public class OpenTSDBStorageTest {

  private IStorage storage;

  private final static String DU = "unit001";

  private final static long START_TIME = 1567675709879L;

  private final static long END_TIME = 1567675710079L;

  private final static String PATH01 = "point_1";

  private final static String PATH02 = "point_2";

  @Before
  public void init() throws PhysicalException {
    connect();
    insertData();
  }

  public void connect() throws StorageInitializationException {
    Map<String, String> map = new HashMap<>();
    map.put("url", "http://127.0.0.1");
    StorageEngineMeta meta = new StorageEngineMeta(1L, "127.0.0.1", 4242, map, "opentsdb", 1L);
    this.storage = new OpenTSDBStorage(meta);
  }

  public void insertData() throws PhysicalException {
    List<String> paths = new ArrayList<>(Arrays.asList(PATH01, PATH02));
    List<DataType> types = new ArrayList<>(Arrays.asList(DataType.LONG, DataType.DOUBLE));
    List<Long> timestamps = new ArrayList<>();

    int size = (int) (END_TIME - START_TIME);
    Object[] valuesList = new Object[size];
    for (long i = 0; i < size; i++) {
      timestamps.add(START_TIME + i);
      Object[] values = new Object[2];
      values[0] = i;
      values[1] = i + 0.1;
      valuesList[(int) i] = values;
    }
    List<Bitmap> bitmapList = new ArrayList<>();
    for (int i = 0; i < timestamps.size(); i++) {
      Object[] values = (Object[]) valuesList[i];
      Bitmap bitmap = new Bitmap(values.length);
      for (int j = 0; j < values.length; j++) {
        if (values[j] != null) {
          bitmap.mark(j);
        }
      }
      bitmapList.add(bitmap);
    }
    RawData rawData = new RawData(paths, timestamps, valuesList, types, bitmapList,
        RawDataType.Row);
    DataView dataView = new RowDataView(rawData, 0, paths.size(), 0, size);
    FragmentMeta fragment = new FragmentMeta(null, null, START_TIME, END_TIME);
    FragmentSource source = new FragmentSource(fragment);
    StoragePhysicalTask task = new StoragePhysicalTask(
        Collections.singletonList(new Insert(source, dataView)));
    task.setStorageUnit(DU);
    this.storage.execute(task);
  }

  @Test
  public void testProject() throws PhysicalException {
    FragmentMeta fragment = new FragmentMeta(null, null, START_TIME - 20, END_TIME + 20);
    Source source = new FragmentSource(fragment);
    List<String> paths = new ArrayList<>(Arrays.asList(PATH01, PATH02));
    project(source, paths);
  }

  private void project(Source source, List<String> paths) throws PhysicalException {
    StoragePhysicalTask task = new StoragePhysicalTask(
        Collections.singletonList(new Project(source, paths)));
    task.setStorageUnit(DU);
    RowStream rowStream = this.storage.execute(task).getRowStream();
    Header header = rowStream.getHeader();
    Row row;
    while (rowStream.hasNext()) {
      row = rowStream.next();
      System.out.println(row);
    }
    System.out.println(header);
    rowStream.close();
  }

  @Test
  public void testDelete() throws PhysicalException, InterruptedException {
    List<String> patterns = new ArrayList<>(Arrays.asList(PATH01, PATH02));
    List<TimeRange> timeRanges = new ArrayList<>(
        Collections.singletonList(new TimeRange(START_TIME + 20, END_TIME - 20)));
    FragmentMeta fragment = new FragmentMeta(null, null, START_TIME, END_TIME);
    FragmentSource source = new FragmentSource(fragment);
    StoragePhysicalTask task = new StoragePhysicalTask(
        Collections.singletonList(new Delete(source, timeRanges, patterns)));
    task.setStorageUnit(DU);
    this.storage.execute(task);

    Thread.sleep(3000);

    fragment = new FragmentMeta(null, null, START_TIME - 50, END_TIME + 50);
    source = new FragmentSource(fragment);
    List<String> paths = new ArrayList<>(Arrays.asList(PATH01, PATH02));
    project(source, paths);
  }
}
