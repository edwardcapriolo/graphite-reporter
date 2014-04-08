package io.teknek.graphite;

/**
 * metrics library has many metrics like 97th percentile which you 
 * may not need EntryFilter filters theses things from being sent
 * 
 */ 
public interface EntryFilter {

  static EntryFilter DEFAULT = new DefaultEntryFilter();

  static EntryFilter ALL = new EntryFilter(){
    @Override
    public boolean shouldSend(String statName) {
      return true; 
    }
  };

  /**
   * @param enty
   *          The last part of stat COUNT, MEAN, 95th etc
   * @return true to include entry false otherwise
   */
  boolean shouldSend(String entry);

}




