package step.core.timeseries;

import java.util.LinkedHashMap;
import java.util.Map;

public class TimeSeriesUtils {

    private static final LinkedHashMap<Long, String> TIME_UNITS = new LinkedHashMap<>();

    static {
        TIME_UNITS.put(365L * 24 * 60 * 60 * 1000, "Year(s)");
        TIME_UNITS.put(31L * 24 * 60 * 60 * 1000, "Month(s)");
        TIME_UNITS.put(7L * 24 * 60 * 60 * 1000, "Week(s)");
        TIME_UNITS.put(24L * 60 * 60 * 1000, "Day(s)");
        TIME_UNITS.put(60L * 60 * 1000, "Hour(s)");
        TIME_UNITS.put(60L * 1000, "Minute(s)");
        TIME_UNITS.put(1000L, "Second(s)");
    }

    public static String formatMilliseconds(long ms) {
        for (Map.Entry<Long, String> entry : TIME_UNITS.entrySet()) {
            long unitMs = entry.getKey();
            if (ms % unitMs == 0) {
                long value = ms / unitMs;
                return value + " " + entry.getValue();
            }
        }
        return ms + " milliseconds";
    }

}
