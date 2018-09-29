package me.nanerlee.storm;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.topology.TopologyBuilder;

public class StormKafkaSecurityTopology {

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        String servers = "NM-304-SA5212M4-BIGDATA-673:9091";
        String inTopicName = "storm-security-test-in";
        String outTopicName = "storm-security-test-out";

        KafkaSpout kafkaSpout = StormKafkaFunctions.createKafkaSpout(servers, inTopicName, true);
        KafkaBolt kafkaBolt = StormKafkaFunctions.createKafkaBolt(servers, outTopicName, true);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout", kafkaSpout);
        builder.setBolt("addNumBolt", new AddNumBolt()).shuffleGrouping("kafkaSpout");
        builder.setBolt("kafkaBolt", kafkaBolt).shuffleGrouping("addNumBolt");

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(1);
        String topologyName = "storm-kafka-security-topology";
        StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());

    }
}