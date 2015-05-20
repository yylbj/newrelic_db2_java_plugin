package com.newrelic.plugins.db2;

import static com.newrelic.plugins.db2.util.Constants.*;

import com.newrelic.metrics.publish.processors.EpochCounter;

/**
 * This class holds additional meta data about a given metric.
 * 
 * Currently a Metric can have the following attributes
 * 
 * - Counter (Yes/No). The default is Yes - Unit Name
 * 
 * @author yylbj@cn.ibm.com
 * 
 */
public class MetricMeta {

    public final static String DEFAULT_UNIT = "Operations";
    public final static String DEFAULT_COUNTER_UNIT = DEFAULT_UNIT + "/Second";

    private final String unit;
    private EpochCounter counter = null;

    public MetricMeta(boolean isCounter, String unit) {
        this.unit = unit;
        if (isCounter) {
            this.counter = new EpochCounter();
        }
    }

    public MetricMeta(boolean isCounter) {
        this.unit = isCounter ? DEFAULT_COUNTER_UNIT : DEFAULT_UNIT;
        if (isCounter) {
            this.counter = new EpochCounter();
        }
    }

    public static MetricMeta defaultMetricMeta() {
        return new MetricMeta(true);
    }

    public boolean isCounter() {
        return (this.counter == null ? false : true);
    }

    public String getUnit() {
        return this.unit;
    }

    public EpochCounter getCounter() {
        return this.counter;
    }

    @Override
    public String toString() {
        return new StringBuilder()
            .append(isCounter() ? COUNTER : EMPTY_STRING)
            .append(LEFT_PAREN)
            .append(getUnit())
            .append(RIGHT_PAREN)
            .toString();
    }
}
