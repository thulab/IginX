package cn.edu.tsinghua.iginx.session;

public class CurveMatchResult {

    private final long matchedTimestamp;
    private final String matchedPath;

    public CurveMatchResult(long matchedTimestamp, String matchedPath) {
        this.matchedTimestamp = matchedTimestamp;
        this.matchedPath = matchedPath;
    }

    public long getMatchedTimestamp() {
        return matchedTimestamp;
    }

    public String getMatchedPath() {
        return matchedPath;
    }

    @Override
    public String toString() {
        return "CurveMatchResult{" +
            "matchedTimestamp=" + matchedTimestamp +
            ", matchedPath='" + matchedPath + '\'' +
            '}';
    }
}
