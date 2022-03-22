package com.study.kafkastreams;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class WordCountService {


    @Bean
            void kafkaStreamBuilder() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> wordCountInput = builder.stream("word-count-input");
        wordCountInput.mapValues(value -> value.toLowerCase());
     }



}
