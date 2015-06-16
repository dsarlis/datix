package gr.ntua.cslab.streamnet;

import gr.ntua.cslab.streamnet.bolts.SFlowBolt;
import gr.ntua.cslab.streamnet.kafka.KafkaProducer;

import java.io.InputStream;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.bridge.SLF4JBridgeHandler;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class Main {
	
	static CountDownLatch topologyStartedLatch = new CountDownLatch(1);
	
	private static final String TOPIC_NAME = "sflows";
    private static KafkaProducer kafkaProducer;
	
	private static void configureLogger() {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
        InputStream logPropertiesStream = Main.class.getClassLoader().getResourceAsStream("log4j.properties");
        PropertyConfigurator.configure(logPropertiesStream);
        Logger.getLogger(Main.class.getName()).info("Logger configured");
    } 
	
	public static void main(String[] args) throws Exception {
        configureLogger();
        
        kafkaProducer = new  KafkaProducer(TOPIC_NAME, topologyStartedLatch, "localhost:9092", "tmp/file");
        kafkaProducer.startKafkaServer();
        kafkaProducer.createTopic(TOPIC_NAME, "1", "1");

        // create and start Storm Topology
        BrokerHosts brokerHosts = new ZkHosts("master:2181");

        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, TOPIC_NAME, "", "storm");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("word", new KafkaSpout(kafkaConfig), 10);
        builder.setBolt("worker1", new SFlowBolt("worker1", "sflows_with_tree_partitioned"), 3).shuffleGrouping("word");
        builder.setBolt("worker1", new SFlowBolt("worker2", ""), 2).shuffleGrouping("exclaim1");

        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
          // remote cluster
          conf.setNumWorkers(3);

          StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
          Logger.getLogger(Main.class.getName()).info("StreamNet is Started!");
        }
        else {

          // local mode
          LocalCluster cluster = new LocalCluster();
          cluster.submitTopology("test", conf, builder.createTopology());
          Logger.getLogger(Main.class.getName()).info("StreamNet is Started!");
          Utils.sleep(10000);
          cluster.killTopology("test");
          cluster.shutdown();
        }
    }
}
