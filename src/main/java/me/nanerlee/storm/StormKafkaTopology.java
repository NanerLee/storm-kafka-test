package me.nanerlee.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;


public class StormKafkaTopology {

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        String servers = "kafka-server1:9092";
        String inTopicName = "storm-test-in";
        String outTopicName = "storm-test-out";

        KafkaSpout kafkaSpout = StormKafkaFunctions.createKafkaSpout(servers, inTopicName, false);
        KafkaBolt kafkaBolt = StormKafkaFunctions.createKafkaBolt(servers, outTopicName, false);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout", kafkaSpout);
        builder.setBolt("addNumBolt", new AddNumBolt()).shuffleGrouping("kafkaSpout");
        builder.setBolt("kafkaBolt", kafkaBolt).shuffleGrouping("addNumBolt");

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(1);
        String topologyName = "storm-kafka-topology";
        StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology(topologyName, conf, builder.createTopology());
//        Utils.sleep(10000);
//        cluster.killTopology(topologyName);
//        cluster.shutdown();
    }

}




