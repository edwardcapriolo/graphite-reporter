package io.teknek.graphite;

import java.util.List;

/*
 * turn only last period to underscore 
 */
public class OnlyFlattenLastTransform extends PrefixStripSuffixTransform {

  public OnlyFlattenLastTransform(String prefix, List<String> toStrip, String suffix) {
    super(prefix, toStrip, suffix);
  }

  protected String cleanMetricName(String metricName) {
    String clean = metricName;
    if (toStrip != null) {
      for (String strip : toStrip) {
        if (clean.startsWith(strip)) {
          clean = clean.substring(strip.length());
          break;
        }
      }
    }

    // strip off leading '.'s (hopefully just one at most)
    for (int i = 0; i < clean.length(); i++) {
      if (clean.charAt(i) == '.') {
        continue;
      } else if (i > 0) {
        clean = clean.substring(i);
        break;
      } else {
        break;
      }
    }

    // replace last '.' with '_' so browsing is slightly less annoying
    int indexOfFinalPeriod = clean.lastIndexOf('.');
    if (indexOfFinalPeriod != -1) {
      clean = clean.substring(0, indexOfFinalPeriod) + "_"
              + clean.substring(indexOfFinalPeriod + 1);
    }
    return clean;
  }

}
