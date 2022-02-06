package com.github.tomboyo.kafkastreamsdemo;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.producerPrefix;

public class AppConfig {
  public static Properties loadProperties(String path) throws IOException {
    var properties = new Properties();
    try (var stream = Files.newInputStream(Path.of(path))) {
      properties.load(stream);
    }
    return properties;
  }

  public static Properties kafkaStreamsProperties() {
    var props = new Properties();
    // Essentially the consumer group.
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-demo");
    // Enables EOS, which sets isolation.level=read_committed and enable.idempotence=true
    props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
    // Await acks from all replicas when producing messages.
    props.put(producerPrefix(ProducerConfig.ACKS_CONFIG), "all");
    return props;
  }

  public static Properties standaloneProducerProperties() {
    var props = new Properties();
    props.putAll(
        Map.of(
            "key.serializer", "org.apache.kafka.common.serialization.LongSerializer",
            "value.serializer", "org.apache.kafka.common.serialization.StringSerializer"));
    return props;
  }

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

  public static Properties mergeProperties(Properties lowPrecedence, Properties... highPrecedence) {
    var result = new Properties();
    result.putAll(lowPrecedence);
    for (var p : highPrecedence) {
      result.putAll(p);
    }
    return result;
  }
}
