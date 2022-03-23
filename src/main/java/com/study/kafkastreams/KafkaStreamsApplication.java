package com.study.kafkastreams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;
import java.util.Properties;

@SpringBootApplication
public class KafkaStreamsApplication {

	public static void main(String[] args) {

		Properties properties = new Properties();
		properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream-word-count");
		properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> wordCountInput = builder.stream("wordcount-count-input");

		KTable<String, Long> wordCounts = wordCountInput.mapValues(textLine -> textLine.toLowerCase())
				.flatMapValues(lowerCaseTextLine -> Arrays.asList(lowerCaseTextLine.split(" ")))
				.selectKey((ignoredKey, word) -> word)
				.groupByKey()
				.count();

		wordCounts.toStream().to("wordcount-count-output", Produced.with(Serdes.String(), Serdes.Long()));


		final Topology topology = builder.build();
		final KafkaStreams streams = new KafkaStreams(topology, properties);
		System.out.println(topology.describe());
		streams.start();


		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

//		SpringApplication.run(KafkaStreamsApplication.class, args);
	}

}
