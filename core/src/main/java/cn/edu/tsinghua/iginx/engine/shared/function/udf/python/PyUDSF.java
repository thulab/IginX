package cn.edu.tsinghua.iginx.engine.shared.function.udf.python;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionType;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.UDSF;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.utils.TypeUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import pemja.core.PythonInterpreter;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static cn.edu.tsinghua.iginx.engine.shared.Constants.*;

public class PyUDSF implements UDSF {

    private static final String PY_UDSF = "py_udsf";

    private final PythonInterpreter interpreter;

    private final String funcName;

    public PyUDSF(PythonInterpreter interpreter, String funcName) {
        this.interpreter = interpreter;
        this.funcName = funcName;
    }

    @Override
    public FunctionType getFunctionType() {
        return FunctionType.UDF;
    }

    @Override
    public MappingType getMappingType() {
        return MappingType.Mapping;
    }

    @Override
    public String getIdentifier() {
        return PY_UDSF;
    }

    @Override
    public RowStream transform(RowStream rows, Map<String, Value> params) throws Exception {
        if (!isLegal(params)) {
            throw new IllegalArgumentException("unexpected params for PyUDAF.");
        }

        String target = params.get(PARAM_PATHS).getBinaryVAsString();
        if (StringUtils.isPattern(target)) {
            Pattern pattern = Pattern.compile(StringUtils.reformatPath(target));
            List<String> name = new ArrayList<>();
            List<Integer> indices = new ArrayList<>();
            for (int i = 0; i < rows.getHeader().getFieldSize(); i++) {
                Field field = rows.getHeader().getField(i);
                if (pattern.matcher(field.getName()).matches()) {
                    name.add(getFunctionName() + "(" + field.getName() + ")");
                    indices.add(i);
                }
            }
            if (name.isEmpty()) {
                return Table.EMPTY_TABLE;
            }

            List<List<Object>> data = new ArrayList<>();
            while (rows.hasNext()) {
                Row row = rows.next();
                List<Object> rowData = new ArrayList<>();
                for (Integer idx: indices) {
                    rowData.add(row.getValues()[idx]);
                }
                data.add(rowData);
            }
            Object[] res = (Object[]) interpreter.invokeMethod(UDF_CLASS, UDF_FUNC, data);
            if (res == null || res.length == 0) {
                return Table.EMPTY_TABLE;
            }

            Object[] firstRow = (Object[])res[0];
            List<Field> targetFields = new ArrayList<>();
            for (int i = 0; i < name.size(); i++) {
                targetFields.add(new Field(name.get(i), TypeUtils.getDataTypeFromObject(firstRow[i])));
            }
            Header header = new Header(targetFields);

            List<Row> rowList = Arrays.stream(res).map(row -> new Row(header, (Object[]) row)).collect(Collectors.toList());
            return new Table(header, rowList);
        } else {
            int index = rows.getHeader().indexOf(target);
            if (index == -1) {
                return Table.EMPTY_TABLE;
            }

            List<List<Object>> data = new ArrayList<>();
            while (rows.hasNext()) {
                Row row = rows.next();
                data.add(Collections.singletonList(row.getValues()[index]));
            }
            Object[] res = (Object[]) interpreter.invokeMethod(UDF_CLASS, UDF_FUNC, data);
            if (res == null || res.length == 0) {
                return Table.EMPTY_TABLE;
            }

            Object[] firstRow = (Object[])res[0];
            Field targetField = new Field(getFunctionName() + "(" + target + ")", TypeUtils.getDataTypeFromObject(firstRow[0]));
            Header header = new Header(Collections.singletonList(targetField));

            List<Row> rowList = Arrays.stream(res).map(row -> new Row(header, (Object[]) row)).collect(Collectors.toList());
            return new Table(header, rowList);
        }
    }

    private boolean isLegal(Map<String, Value> params) {
        List<String> neededParams = Arrays.asList(PARAM_PATHS);
        for (String param : neededParams) {
            if (!params.containsKey(param)) {
                return false;
            }
        }

        Value paths = params.get(PARAM_PATHS);
        if (paths == null || paths.getDataType() != DataType.BINARY) {
            return false;
        }
        return true;
    }

    @Override
    public String getFunctionName() {
        return funcName;
    }
}
