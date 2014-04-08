package io.teknek.graphite;

import java.util.Set;

import static io.teknek.graphite.reporter.Reporter.*;

import com.google.common.collect.Sets;

public class DefaultEntryFilter implements EntryFilter {

  private static final Set<String> EXCLUDES = Sets.newHashSet(

  //Min and max are not graphable and we do not want them        
  MAX, MIN,
  
  //that rate is so 15 minutes ago
  M15_RATE,

  // mean doesn't move much once you've seen enough events.
  MEAN_RATE, MEAN, STDDEV,

  // not very interesting percentiles
  P75, P98);

  @Override
  public boolean shouldSend(String statName) {
    return !EXCLUDES.contains(statName);
  }

}
