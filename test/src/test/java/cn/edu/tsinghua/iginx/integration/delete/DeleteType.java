package cn.edu.tsinghua.iginx.integration.delete;

import java.util.ArrayList;
import java.util.List;

public class DeleteType {
    List<String> inputPath;
    List<String> deletePath;
    int originStart;
    int originEnd;
    int deleteStart;
    int deleteEnd;

    // the type here include 0-3,
    // 0 is delete part data in the partial path
    // 1 is delete all data in the partial path
    // 2 is delete part data in all path
    // 3 is delete all data in all path

    int type;

    public DeleteType(int type, int originStart, int originEnd, List<String> inputPath){
        this.type = type;
        this.originStart = originStart;
        this.originEnd = originEnd;
        this.inputPath = inputPath;
        deletePath = new ArrayList<>();
        getDeletePart();
    }

    private void getDeletePart(){
        int period = originEnd - originStart + 1;
        int start = originStart + period / 10;
        int end = originStart + period * 9 / 10;
        int len = inputPath.size();
        switch (type){
            case 0:
                deleteStart = start;
                deleteEnd = end;
                for(int i = 0; i < len - 1; i++){
                    deletePath.add(inputPath.get(i));
                }
                break;
            case 1:
                deleteStart = originStart;
                deleteEnd = originEnd;
                for(int i = 0; i < len - 1; i++){
                    deletePath.add(inputPath.get(i));
                }
                break;
            case 2:
                deleteStart = start;
                deleteEnd = end;
                deletePath.addAll(inputPath);
                break;
            case 3:
                deleteStart = originStart;
                deleteEnd = originEnd;
                deletePath.addAll(inputPath);
                break;
            default:
                break;
        }
    }

    public int getDeleteStart() {
        return deleteStart;
    }

    public int getDeleteEnd() {
        return deleteEnd;
    }

    public List<String> getDeletePath() {
        return deletePath;
    }

    public int getType() {
        return type;
    }
}
