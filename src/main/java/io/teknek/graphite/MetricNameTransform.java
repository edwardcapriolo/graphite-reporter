package io.teknek.graphite;

/**
 * People can make crazy names. Especially if the java packages
 * gets converted into a very deep tree. This class can guarge against
 * that by re-writing cruddy class names
 * @author edward
 *
 */
public interface MetricNameTransform {

  public static MetricNameTransform NO_TRANSFORM = new MetricNameTransform(){
    @Override
    public String transform(String metricName) {
      return metricName;
    }
  };

  String transform(String metricName);

}
