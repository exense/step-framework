package step.core.timeseries;

import java.util.List;
import java.util.Map;

public class TimeSeriesChartResponse {
    private long start;
    private long interval;
    private long end;

    // all buckets for all series. series keys are defined in the legend
    private List<Bucket[]> matrix;

    // each object index is corresponding to the row in the matrix
    private List<BucketAttributes> matrixKeys;

    public TimeSeriesChartResponse(long start, long end, long interval, List<Bucket[]> matrix, List<BucketAttributes> matrixKeys) {
        this.start = start;
        this.interval = interval;
        this.end = end;
        this.matrix = matrix;
        this.matrixKeys = matrixKeys;
    }

    public long getStart() {
        return start;
    }

    public long getInterval() {
        return interval;
    }

    public long getEnd() {
        return end;
    }

    public List<Bucket[]> getMatrix() {
        return matrix;
    }

    public List<BucketAttributes> getMatrixKeys() {
        return matrixKeys;
    }
}
