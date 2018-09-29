package me.nanerlee.storm;

import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Properties;

class StormKafkaFunctions {

    static KafkaSpout<String, String> createKafkaSpout(String servers, String topicName, boolean isSecurity) {
        Properties props;
        if (isSecurity) {
            props = getSecurityProps();
        } else {
            props = new Properties();
        }
        props.put("bootstrap.servers", servers);
        props.put("group.id","storm-test");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaSpoutConfig<String, String> kafkaSpoutConfig = KafkaSpoutConfig
                .builder(props.getProperty("bootstrap.servers"), topicName)
                .setProp(props)
//                .setRecordTranslator((r) -> new Values(r.topic(), r.key(), r.value()), new Fields("topic", "key", "message"))
                .setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST)
                .build();

        return new KafkaSpout<>(kafkaSpoutConfig);
    }

    static KafkaBolt<String, String> createKafkaBolt(String servers, String topicName, boolean isSecurity) {
        Properties props;
        if (isSecurity) {
            props = getSecurityProps();
        } else {
            props = new Properties();
        }
        props.put("bootstrap.servers", servers);
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaBolt<String, String>()
                .withProducerProperties(props)
                .withTopicSelector(topicName)
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<String, String>("key", "message"));
    }

    private static Properties getSecurityProps() {
        Properties props = new Properties();
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "GSSAPI");
        props.put("sasl.kerberos.service.name", "kafka");
        return props;
    }
}
