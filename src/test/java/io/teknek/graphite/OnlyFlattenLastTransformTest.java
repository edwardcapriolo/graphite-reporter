package io.teknek.graphite;

import org.junit.Test;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class OnlyFlattenLastTransformTest {

  private final String metricName = "io.teknek.graphite.util.totalErrors.p50";

  @Test
  public void testNulls() {
    OnlyFlattenLastTransform trans = new OnlyFlattenLastTransform(null, null, null);
    assertThat(trans.transform(metricName), equalTo("io.teknek.graphite.util.totalErrors_p50"));
  }

  @Test
  public void testClusterAndHostUpFront() {
    OnlyFlattenLastTransform trans = new OnlyFlattenLastTransform("production.web1", null, null);
    assertThat(trans.transform(metricName), equalTo("production.web1.io.teknek.graphite.util.totalErrors_p50"));
  }
}