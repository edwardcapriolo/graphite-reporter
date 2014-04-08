package io.teknek.graphite.reporter;

import io.teknek.graphite.EntryFilter;
import io.teknek.graphite.Graphite;
import io.teknek.graphite.MetricNameTransform;
import io.teknek.graphite.OnlyFlattenLastTransform;
import io.teknek.graphite.reporter.Reporter.Builder;

import java.io.Closeable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Clock;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;

public class SimpleGraphiteReporter implements Closeable {

  protected Reporter reporter;
  protected MetricRegistry registry;
  protected String host;
  protected String graphiteHost;
  protected int graphitePort;
  protected boolean on;
  protected MetricNameTransform transform;
  protected String prefix;
  protected List<String> toStrip;
  protected EntryFilter sendFilter;

  public SimpleGraphiteReporter(MetricRegistry registry, String graphiteHost, int graphitePort,
          boolean on) {
    this.registry = registry;
    this.graphiteHost = graphiteHost;
    this.graphitePort = graphitePort;
    this.on = on;
  }

  /**
   * When auto-deriving the hostname we do not want periods in the hostname. Otherwise graphite will
   * make trees for each period. Replace . with _
   * 
   * @param hostname
   * @return the hostname with period replaced by underscore
   */
  public static String cleanupHostName(String hostname) {
    return hostname.replace('.', '_');
  }

  public void init() {
    if (on) {
      if (host == null) {
        try {
          host = InetAddress.getLocalHost().getHostName();
          host = cleanupHostName(host);
        } catch (UnknownHostException ex) {
          host = "unknown";
        }
      }
      if (transform == null) {
        transform = new OnlyFlattenLastTransform(prefix, toStrip, host);
      }
      if (sendFilter == null) {
        sendFilter = EntryFilter.DEFAULT;
      }
      Graphite graphite = new Graphite(new InetSocketAddress(graphiteHost, graphitePort));
      Builder builder = Reporter.forRegistry(registry);
      builder.convertRatesTo(TimeUnit.SECONDS);
      builder.convertDurationsTo(TimeUnit.MILLISECONDS);
      builder.sendFilter(sendFilter);
      builder.transform(transform);
      builder.filter(MetricFilter.ALL);
      builder.withClock(Clock.defaultClock());
      reporter = builder.build(graphite);
      reporter.start(1, TimeUnit.MINUTES);
    }
  }

  /**
   * Close the reporter if it is not null
   */
  public void close() {
    if (reporter != null) {
      reporter.close();
    }
  }

  public Reporter getReporter() {
    return reporter;
  }

  public MetricRegistry getRegistry() {
    return registry;
  }

  public void setRegistry(MetricRegistry registry) {
    this.registry = registry;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public String getGraphiteHost() {
    return graphiteHost;
  }

  public void setGraphiteHost(String graphiteHost) {
    this.graphiteHost = graphiteHost;
  }

  public int getGraphitePort() {
    return graphitePort;
  }

  public void setGraphitePort(int graphitePort) {
    this.graphitePort = graphitePort;
  }

  public boolean isOn() {
    return on;
  }

  public void setOn(boolean on) {
    this.on = on;
  }

  public MetricNameTransform getTransform() {
    return transform;
  }

  public void setTransform(MetricNameTransform transform) {
    this.transform = transform;
  }

  public void setReporter(Reporter reporter) {
    this.reporter = reporter;
  }

  public String getPrefix() {
    return prefix;
  }

  public void setPrefix(String prefix) {
    this.prefix = prefix;
  }

  public List<String> getToStrip() {
    return toStrip;
  }

  public void setToStrip(List<String> toStrip) {
    this.toStrip = toStrip;
  }

  public EntryFilter getSendFilter() {
    return sendFilter;
  }

  public void setSendFilter(EntryFilter sendFilter) {
    this.sendFilter = sendFilter;
  }

}
