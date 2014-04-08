package io.teknek.graphite.reporter;

import io.teknek.graphite.EntryFilter;
import io.teknek.graphite.Graphite;
import io.teknek.graphite.MetricNameTransform;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

/**
 * A reporter which publishes metric values to a Graphite server.
 *
 * @see <a href="http://graphite.wikidot.com/">Graphite - Scalable Realtime Graphing</a>
 */
public class Reporter extends ScheduledReporter {
    public static final String MEAN_RATE = "mean_rate";
    public static final String M15_RATE = "m15_rate";
    public static final String M5_RATE = "m5_rate";
    public static final String M1_RATE = "m1_rate";
    public static final String COUNT = "count";
    public static final String P999 = "p999";
    public static final String P99 = "p99";
    public static final String P98 = "p98";
    public static final String P95 = "p95";
    public static final String P75 = "p75";
    public static final String P50 = "p50";
    public static final String STDDEV = "stddev";
    public static final String MIN = "min";
    public static final String MEAN = "mean";
    public static final String MAX = "max";

    /**
     * Returns a new {@link Builder} for {@link GraphiteReporter}.
     *
     * @param registry the registry to report
     * @return a {@link Builder} instance for a {@link GraphiteReporter}
     */
    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    /**
     * A builder for {@link GraphiteReporter} instances. Defaults to not using a prefix, using the
     * default clock, converting rates to events/second, converting durations to milliseconds, and
     * not filtering metrics.
     */
    public static class Builder {
        private final MetricRegistry registry;
        private Clock clock;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter;
        private EntryFilter sendFilter;
        private MetricNameTransform transform;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.clock = Clock.defaultClock();
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
            this.sendFilter = EntryFilter.DEFAULT;
            this.transform = MetricNameTransform.NO_TRANSFORM;
        }

        /**
         * Use the given {@link Clock} instance for the time.
         *
         * @param clock a {@link Clock} instance
         * @return {@code this}
         */
        public Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        /**
         * Convert rates to the given time unit.
         *
         * @param rateUnit a unit of time
         * @return {@code this}
         */
        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Convert durations to the given time unit.
         *
         * @param durationUnit a unit of time
         * @return {@code this}
         */
        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        /**
         * Only report metrics which match the given filter.
         *
         * @param filter a {@link MetricFilter}
         * @return {@code this}
         */
        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        /**
         * Only report metrics which match the given filter.
         *
         * @param filter a {@link MetricFilter}
         * @return {@code this}
         */
        public Builder sendFilter(EntryFilter sendFilter) {
            this.sendFilter = sendFilter;
            return this;
        }

        /**
         * Transform the metric name for use with Graphite.
         *
         * @param transform with a {@link GraphiteNameTransform}
         * @return {@code this}
         */
        public Builder transform(MetricNameTransform transform) {
            this.transform = transform;
            return this;
        }

        /**
         * Builds a {@link GraphiteReporter} with the given properties, sending metrics using the
         * given {@link Graphite} client.
         *
         * @param graphite a {@link Graphite} client
         * @return a {@link GraphiteReporter}
         */
        public Reporter build(Graphite graphite) {
            return new Reporter(registry,
                                        graphite,
                                        clock,
                                        rateUnit,
                                        durationUnit,
                                        filter,
                                        sendFilter,
                                        transform);
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(Reporter.class);

    private final Graphite graphite;
    private final Clock clock;
    private final EntryFilter sendFilter;
    private final MetricNameTransform transform;

    private Reporter(MetricRegistry registry,
                             Graphite graphite,
                             Clock clock,
                             TimeUnit rateUnit,
                             TimeUnit durationUnit,
                             MetricFilter filter,
                             EntryFilter sendFilter,
                             MetricNameTransform transform) {
        super(registry, "graphite-reporter", filter, rateUnit, durationUnit);
        this.graphite = graphite;
        this.clock = clock;
        this.sendFilter = sendFilter;
        this.transform = transform;
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {
        final long timestamp = clock.getTime() / 1000;

        // oh it'd be lovely to use Java 7 here
        try {
            graphite.connect();

            for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
                reportGauge(entry.getKey(), entry.getValue(), timestamp);
            }

            for (Map.Entry<String, Counter> entry : counters.entrySet()) {
                reportCounter(entry.getKey(), entry.getValue(), timestamp);
            }

            for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
                reportHistogram(entry.getKey(), entry.getValue(), timestamp);
            }

            for (Map.Entry<String, Meter> entry : meters.entrySet()) {
                reportMetered(entry.getKey(), entry.getValue(), timestamp);
            }

            for (Map.Entry<String, Timer> entry : timers.entrySet()) {
                reportTimer(entry.getKey(), entry.getValue(), timestamp);
            }
        } catch (IOException e) {
            LOGGER.warn("Unable to report to Graphite", graphite, e);
        } finally {
            try {
                graphite.close();
            } catch (IOException e) {
                LOGGER.debug("Error disconnecting from Graphite", graphite, e);
            }
        }
    }

    private void reportTimer(String name, Timer timer, long timestamp) throws IOException {
        final Snapshot snapshot = timer.getSnapshot();

        if(sendFilter.shouldSend(MAX)) {
            graphite.send(name(name, MAX), format(convertDuration(snapshot.getMax())), timestamp);
        }
        if(sendFilter.shouldSend(MEAN)) {
            graphite.send(name(name, MEAN), format(convertDuration(snapshot.getMean())), timestamp);
        }
        if(sendFilter.shouldSend(MIN)) {
            graphite.send(name(name, MIN), format(convertDuration(snapshot.getMin())), timestamp);
        }
        if(sendFilter.shouldSend(STDDEV)) {
            graphite.send(name(name, STDDEV),
                          format(convertDuration(snapshot.getStdDev())),
                          timestamp);
        }
        if(sendFilter.shouldSend(P50)) {
            graphite.send(name(name, P50),
                          format(convertDuration(snapshot.getMedian())),
                          timestamp);
        }
        if(sendFilter.shouldSend(P75)) {
            graphite.send(name(name, P75),
                          format(convertDuration(snapshot.get75thPercentile())),
                          timestamp);
        }
        if(sendFilter.shouldSend(P95)) {
            graphite.send(name(name, P95),
                          format(convertDuration(snapshot.get95thPercentile())),
                          timestamp);
        }
        if(sendFilter.shouldSend(P98)) {
            graphite.send(name(name, P98),
                          format(convertDuration(snapshot.get98thPercentile())),
                          timestamp);
        }
        if(sendFilter.shouldSend(P99)) {
            graphite.send(name(name, P99),
                          format(convertDuration(snapshot.get99thPercentile())),
                          timestamp);
        }
        if(sendFilter.shouldSend(P999)) {
            graphite.send(name(name, P999),
                          format(convertDuration(snapshot.get999thPercentile())),
                          timestamp);
        }

        reportMetered(name, timer, timestamp);
    }

    private void reportMetered(String name, Metered meter, long timestamp) throws IOException {
        if(sendFilter.shouldSend(COUNT)) {
            graphite.send(name(name, COUNT), format(meter.getCount()), timestamp);
        }
        if(sendFilter.shouldSend(M1_RATE)) {
            graphite.send(name(name, M1_RATE),
                          format(convertRate(meter.getOneMinuteRate())),
                          timestamp);
        }
        if(sendFilter.shouldSend(M5_RATE)) {
            graphite.send(name(name, M5_RATE),
                          format(convertRate(meter.getFiveMinuteRate())),
                          timestamp);
        }
        if(sendFilter.shouldSend(M15_RATE)) {
            graphite.send(name(name, M15_RATE),
                          format(convertRate(meter.getFifteenMinuteRate())),
                          timestamp);
        }
        if(sendFilter.shouldSend(MEAN_RATE)) {
            graphite.send(name(name, MEAN_RATE),
                          format(convertRate(meter.getMeanRate())),
                          timestamp);
        }
    }

    private void reportHistogram(String name, Histogram histogram, long timestamp) throws IOException {
        final Snapshot snapshot = histogram.getSnapshot();
        if(sendFilter.shouldSend(COUNT)) {
            graphite.send(name(name, COUNT), format(histogram.getCount()), timestamp);
        }
        if(sendFilter.shouldSend(MAX)) {
            graphite.send(name(name, MAX), format(snapshot.getMax()), timestamp);
        }
        if(sendFilter.shouldSend(MEAN)) {
            graphite.send(name(name, MEAN), format(snapshot.getMean()), timestamp);
        }
        if(sendFilter.shouldSend(MIN)) {
            graphite.send(name(name, MIN), format(snapshot.getMin()), timestamp);
        }
        if(sendFilter.shouldSend(STDDEV)) {
            graphite.send(name(name, STDDEV), format(snapshot.getStdDev()), timestamp);
        }
        if(sendFilter.shouldSend(P50)) {
            graphite.send(name(name, P50), format(snapshot.getMedian()), timestamp);
        }
        if(sendFilter.shouldSend(P75)) {
            graphite.send(name(name, P75), format(snapshot.get75thPercentile()), timestamp);
        }
        if(sendFilter.shouldSend(P95)) {
            graphite.send(name(name, P95), format(snapshot.get95thPercentile()), timestamp);
        }
        if(sendFilter.shouldSend(P98)) {
            graphite.send(name(name, P98), format(snapshot.get98thPercentile()), timestamp);
        }
        if(sendFilter.shouldSend(P99)) {
            graphite.send(name(name, P99), format(snapshot.get99thPercentile()), timestamp);
        }
        if(sendFilter.shouldSend(P999)) {
            graphite.send(name(name, P999), format(snapshot.get999thPercentile()), timestamp);
        }
    }

    private void reportCounter(String name, Counter counter, long timestamp) throws IOException {
        if(sendFilter.shouldSend(COUNT)) {
            graphite.send(name(name, COUNT), format(counter.getCount()), timestamp);
        }
    }

    private void reportGauge(String name, Gauge gauge, long timestamp) throws IOException {
        final String value = format(gauge.getValue());
        if (value != null) {
            graphite.send(name(name), value, timestamp);
        }
    }

    private String format(Object o) {
        if (o instanceof Float) {
            return format(((Float) o).doubleValue());
        } else if (o instanceof Double) {
            return format(((Double) o).doubleValue());
        } else if (o instanceof Byte) {
            return format(((Byte) o).longValue());
        } else if (o instanceof Short) {
            return format(((Short) o).longValue());
        } else if (o instanceof Integer) {
            return format(((Integer) o).longValue());
        } else if (o instanceof Long) {
            return format(((Long) o).longValue());
        }
        return null;
    }

    private String name(String s1, String... rest) {
        return transform.transform(MetricRegistry.name(s1, rest));
    }

    private String format(long n) {
        return Long.toString(n);
    }

    private String format(double v) {
        // the Carbon plaintext format is pretty underspecified, but it seems like it just wants
        // US-formatted digits
        return String.format(Locale.US, "%2.2f", v);
    }
}
