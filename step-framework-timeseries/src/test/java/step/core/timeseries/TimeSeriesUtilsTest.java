package step.core.timeseries;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TimeSeriesUtilsTest {

    @Test
    public void testTimeFormatterFunction() {

        assertEquals("999 milliseconds", TimeSeriesUtils.formatMilliseconds(999));
        assertEquals("45500 milliseconds", TimeSeriesUtils.formatMilliseconds(45500));
        assertEquals("1001 milliseconds", TimeSeriesUtils.formatMilliseconds(1001));

        assertEquals("1 Second(s)", TimeSeriesUtils.formatMilliseconds(1000));
        assertEquals("30 Second(s)", TimeSeriesUtils.formatMilliseconds(30000));
        assertEquals("61 Second(s)", TimeSeriesUtils.formatMilliseconds(61000));

        assertEquals("1 Minute(s)", TimeSeriesUtils.formatMilliseconds(60000));
        assertEquals("2 Minute(s)", TimeSeriesUtils.formatMilliseconds(120000));
        assertEquals("61 Minute(s)", TimeSeriesUtils.formatMilliseconds(60000 * 61));

        assertEquals("1 Hour(s)", TimeSeriesUtils.formatMilliseconds(3600000));
        assertEquals("5 Hour(s)", TimeSeriesUtils.formatMilliseconds(18000000));

        assertEquals("1 Day(s)", TimeSeriesUtils.formatMilliseconds(86400000));
        assertEquals("2 Day(s)", TimeSeriesUtils.formatMilliseconds(2 * 86400000L));
        assertEquals("366 Day(s)", TimeSeriesUtils.formatMilliseconds(366L * 86400000L));

        assertEquals("1 Week(s)", TimeSeriesUtils.formatMilliseconds(7 * 86400000L));
        assertEquals("3 Week(s)", TimeSeriesUtils.formatMilliseconds(3 * 7 * 86400000L));

        assertEquals("1 Month(s)", TimeSeriesUtils.formatMilliseconds(31L * 86400000L));

        assertEquals("1 Year(s)", TimeSeriesUtils.formatMilliseconds(365L * 86400000L));
        assertEquals("2 Year(s)", TimeSeriesUtils.formatMilliseconds(2L * 365 * 86400000L));

    }

}
