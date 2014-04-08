package io.teknek.graphite;

import java.util.Collections;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

public class PrefixStripSuffixTransform implements MetricNameTransform {

    protected final List<String> toStrip;
    protected final String prefix;
    protected final String suffix;

    public PrefixStripSuffixTransform(String prefix, List<String> toStrip, String suffix) {
        if(toStrip == null) {
            this.toStrip = Collections.emptyList();
        } else {
            this.toStrip = Lists.transform(toStrip, new Function<String, String>() {
                public String apply(String input) {
                    return input.trim();
                }
            });
        }
        this.prefix = Strings.nullToEmpty(prefix).trim();
        this.suffix = Strings.nullToEmpty(suffix).trim();
    }

    @Override
    public String transform(String metricName) {
        StringBuilder out = new StringBuilder();

        if(!Strings.isNullOrEmpty(prefix)) {
            out.append(prefix).append('.');
        }

        String clean = cleanMetricName(metricName);
        out.append(clean);

        if(!Strings.isNullOrEmpty(suffix)) {
            out.append('.').append(suffix);
        }

        return out.toString();
    }

    protected String cleanMetricName(String metricName) {
        String clean = metricName;
        if(toStrip != null) {
            for(String strip: toStrip) {
                if(clean.startsWith(strip)) {
                    clean = clean.substring(strip.length());
                    break;
                }
            }
        }

        //strip off leading '.'s (hopefully just one at most)
        for(int i=0; i<clean.length(); i++) {
            if(clean.charAt(i) == '.') {
                continue;
            } else if(i > 0) {
                clean = clean.substring(i);
                break;
            } else {
                break;
            }
        }

        //replace remaining '.' with '_' so browsing in graphite isn't so annoying.
        clean = clean.replace('.', '_');
        return clean;
    }
}
