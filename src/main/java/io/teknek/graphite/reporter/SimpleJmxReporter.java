package io.teknek.graphite.reporter;

import java.io.Closeable;
import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.JmxReporter.Builder;
import com.codahale.metrics.MetricRegistry;

public class SimpleJmxReporter implements Closeable {

  private JmxReporter reporter;
  private MetricRegistry registry;
  private String domain;

  public SimpleJmxReporter(MetricRegistry registry, String domain) {
    this.registry = registry;
    this.domain = domain;
  }

  public void init() {
    Builder builder = JmxReporter.forRegistry(registry).registerWith(
            ManagementFactory.getPlatformMBeanServer());
    if (domain != null) {
      builder.inDomain(domain);
    }
    builder.convertRatesTo(TimeUnit.SECONDS);
    builder.convertDurationsTo(TimeUnit.MILLISECONDS);
    builder.filter(MetricFilter.ALL);
    reporter = builder.build();
    reporter.start();
  }

  public void close() {
    if (reporter != null) {
      reporter.close();
    }
  }

  public JmxReporter getReporter() {
    return reporter;
  }

}
