package com.github.tomboyo.kafkastreamsdemo;

import java.util.Map;
import java.util.Properties;

public class PropertySupport {
  public static Properties subproperties(Properties properties, String prefix) {
    var p = prefix + (prefix.endsWith(".") ? "" : ".");
    return properties.keySet().stream()
        .filter(key -> ((String) key).startsWith(p))
        .collect(
            Properties::new,
            (acc, key) ->
                acc.put(
                    ((String) key).substring(p.length()), // Key without the prefix
                    properties.get(key)),
            Properties::putAll);
  }

  @SafeVarargs
  public static Properties mergeProperties(Properties lowPrecedence, Map<Object, Object>... highPrecedence) {
    var result = new Properties();
    result.putAll(lowPrecedence);
    for (var p : highPrecedence) {
      result.putAll(p);
    }
    return result;
  }
}
