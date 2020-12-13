package io.woolford;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Application {

    public static void main(String[] args) {

        // set props for Kafka Steams app (see KafkaConstants)
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, KafkaConstants.APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.KAFKA_BROKERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> piiDataStream = builder.stream("pii_data_ssn");

        piiDataStream.mapValues(value -> {
            value = Application.piiMasker(value);
            return value;
        }).to("delete-me");

        // run it
        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    private static String piiMasker(String piiData) {

        ObjectMapper mapper = new ObjectMapper();
        PiiDataRecord piiDataRecord = new PiiDataRecord();
        try {
            piiDataRecord = mapper.readValue(piiData, PiiDataRecord.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        String pattern = "[0-9]{3}-[0-9]{2}-([0-9]{4})";
        String ssn = piiDataRecord.getSsn();
        Pattern compiledPattern = Pattern.compile(pattern);
        Matcher matcher = compiledPattern.matcher(ssn);

        if (matcher.find()){
            ssn = "***-**-" + matcher.group(1);
        }

        piiDataRecord.setSsn(ssn);

        String piiDataRecordJson = null;
        try {
            piiDataRecordJson = mapper.writeValueAsString(piiDataRecord);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        return piiDataRecordJson;
    }

}
