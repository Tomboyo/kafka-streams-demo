package com.github.tomboyo.kafkastreamsdemo;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

import static com.github.tomboyo.kafkastreamsdemo.PropertySupport.mergeProperties;
import static com.github.tomboyo.kafkastreamsdemo.PropertySupport.subproperties;
import static org.apache.kafka.streams.StreamsConfig.producerPrefix;

public class AppConfig {
  public static Properties loadProperties(String path) throws IOException {
    var properties = new Properties();
    try (var stream = Files.newInputStream(Path.of(path))) {
      properties.load(stream);
    }
    return properties;
  }

  public static Admin createAdmin(Properties p) {
    return Admin.create(kafkaAll(p));
  }

  public static KafkaProducer<Long, String> createProducer(Properties p) {
    return new KafkaProducer<>(mergeProperties(kafkaAll(p), Map.of(
        "key.serializer", "org.apache.kafka.common.serialization.LongSerializer",
        "value.serializer", "org.apache.kafka.common.serialization.StringSerializer"
    )));
  }

  public static KafkaStreams createKafkaStreams(Topology topology, Properties p) {
    return new KafkaStreams(
            topology,
            mergeProperties(kafkaAll(p), Map.of(
                StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-demo",
                StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2,
                producerPrefix(ProducerConfig.ACKS_CONFIG), "all"
            )));
  }

  private static Properties kafkaAll(Properties p) {
    return subproperties(p, "app.kafka.all");
  }
}
