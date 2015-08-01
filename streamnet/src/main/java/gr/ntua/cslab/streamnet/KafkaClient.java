package gr.ntua.cslab.streamnet;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaClient {
	private static Properties props;
	private static final String TOPIC_NAME = "sflows";
	private static String FILENAME;
	
	private static void loadProperties() {
		props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092, master:9093");
		props.put(ProducerConfig.RETRIES_CONFIG, "3");
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 200);
		props.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, true);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
				"org.apache.kafka.common.serialization.StringSerializer");
	}
	
	public static void main(String[] args) throws Exception {
		/*
		 *  Usage: KafkaClient path_to_file partitionNumber cutoff
		 * */
		
		loadProperties();
		
		FILENAME = args[0];
		int partitionNo = Integer.parseInt(args[1]);
		int cutoff = Integer.parseInt(args[2]);
		
		System.out.println("******" + cutoff + "*******");
		
		KafkaProducer<String, String> kp = new KafkaProducer<String, String>(props);
		
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(
	        		new GZIPInputStream(new FileInputStream(FILENAME)), "UTF-8"));
			String line;
			line = br.readLine();
			int id = 0;
			/*while (line != null) {
				int count = 0;
				String records = "";
				while (count < batchSize && line != null) {
					records += line + ",";
					line = br.readLine();
					count++;
				}
				records = records.substring(0, records.length()-1);
				ProducerRecord<String, String> data = 
						new ProducerRecord<String, String>(TOPIC_NAME, id, ""+id, records);
				kp.send(data);
				id++;
				if (id > partitionNo-1)
					id = 0;
			}*/

			int count = 0;
			while (line != null) {
				count++;
				ProducerRecord<String, String> data = 
						new ProducerRecord<String, String>(TOPIC_NAME, id, ""+id, line);
//				ProducerRecord<String, String> data = 
//						new ProducerRecord<String, String>(TOPIC_NAME, id, ""+id, "mitsos"+count);
				kp.send(data);
				line = br.readLine();
				id++;
				if (id > partitionNo-1)
					id = 0;
				if (count > cutoff)
					break;
			}
			br.close();
		    kp.close();
		} catch (IOException e) {
			System.err.println("Failed: Could not read sFlow file!");
		}
    }
}
