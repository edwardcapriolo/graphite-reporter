package io.teknek.graphite.reporter;

import io.teknek.graphite.EntryFilter;
import io.teknek.graphite.Graphite;
import io.teknek.graphite.OnlyFlattenLastTransform;
import io.teknek.graphite.reporter.Reporter.Builder;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Clock;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;

public class CommonGraphiteReporter extends SimpleGraphiteReporter {

  protected String clusterName;
  
  public CommonGraphiteReporter(MetricRegistry registry, String graphiteHost,
          int graphitePort, boolean on) {
    super(registry, graphiteHost, graphitePort, on);
  }
  
  public void init() {
    if (on) {
      if (host == null) {
        try {
          host = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException ex) {
          host = "unknown";
        }
      }
      host = cleanupHostName(host);
      if (clusterName != null){
        prefix = clusterName + "." + host;
      } else{
        prefix = host;
      }
      if (transform == null) {
        transform = new OnlyFlattenLastTransform(prefix, toStrip, null);
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

  public String getClusterName() {
    return clusterName;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }
  
} 
