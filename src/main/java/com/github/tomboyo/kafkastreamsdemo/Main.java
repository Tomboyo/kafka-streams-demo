package com.github.tomboyo.kafkastreamsdemo;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.tomboyo.kafkastreamsdemo.AppConfig.createAdmin;
import static com.github.tomboyo.kafkastreamsdemo.AppConfig.createKafkaStreams;
import static com.github.tomboyo.kafkastreamsdemo.AppConfig.createProducer;
import static com.github.tomboyo.kafkastreamsdemo.AppConfig.loadProperties;
import static org.apache.kafka.common.config.TopicConfig.RETENTION_BYTES_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.RETENTION_MS_CONFIG;

public class Main {

  public static void main(String[] args) throws Exception {
    var props = loadProperties("./config/application.properties");

    var logger = LoggerFactory.getLogger(Main.class);

    try (var admin = createAdmin(props)) {
      admin.createTopics(topics());
    }

    try (var producer = createProducer(props)) {
      emitInputs(producer);
    }

    var latch = new CountDownLatch(1);
    var streams = createKafkaStreams(
        createTopology(logger),
        props);

    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    // When streams shut down for any reason (i.e. shutdown hook or an untapped exception),
    // decrement the latch so that the main thread can exit.
    streams.setStateListener(
        (newState, oldState) -> {
          if (newState.hasCompletedShutdown()) {
            latch.countDown();
          }
        });

    streams.start();
    latch.await();
  }

  private static List<NewTopic> topics() {
    return Stream.of(
            new NewTopic("input-high", Optional.of(2), Optional.empty()),
            new NewTopic("input-low", Optional.of(2), Optional.empty()),
            new NewTopic("input-high.DLT", Optional.empty(), Optional.empty()),
            new NewTopic("input-low.DLT", Optional.empty(), Optional.empty()),
            new NewTopic("output-left", Optional.empty(), Optional.empty()),
            new NewTopic("output-right", Optional.empty(), Optional.empty()))
        .map(
            nt ->
                nt.configs(
                    Map.of(
                        RETENTION_MS_CONFIG, "10000",
                        RETENTION_BYTES_CONFIG, "1024")))
        .collect(Collectors.toList());
  }

  public static void emitInputs(KafkaProducer<Long, String> producer)
      throws InterruptedException, ExecutionException {
    var futures = new Future<?>[10];
    for (int i = 0; i < 10; i++) {
      var topic = i % 2 == 0 ? "input-high" : "input-low";
      var content = i + (i % 4 < 2 ? "-OK" : "-ERROR");
      futures[i] = producer.send(new ProducerRecord<>(topic, (long) i, content));
    }

    for (int i = 0; i < 10; i++) {
      futures[i].get();
    }
  }

  public static Topology createTopology(Logger logger) {
    var builder = new StreamsBuilder();
    builder
        .stream(List.of("input-low", "input-high"), Consumed.with(Serdes.Long(), Serdes.String()))
        .peek((key, value) -> {
          logger.info("Processing {}", value);
          if (value.contains("ERROR")) {
            throw new RuntimeException("Simulated error!");
          }
        })
        .flatMapValues(value -> List.of(value + "-LEFT", value + "-RIGHT"))
        .peek((key, value) -> logger.info("Producing {}", value))
        .to(
            (key, value, context) -> value.endsWith("-LEFT") ? "output-left" : "output-right",
            Produced.with(Serdes.Long(), Serdes.String()));
    return builder.build();
  }
}
