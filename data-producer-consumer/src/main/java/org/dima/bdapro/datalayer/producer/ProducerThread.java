package org.dima.bdapro.datalayer.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.dima.bdapro.datalayer.bean.Transaction;
import org.dima.bdapro.datalayer.bean.json.TransactionSerializer;
import org.dima.bdapro.datalayer.generator.DataGenerator;
import org.dima.bdapro.utils.PropertiesHandler;

import java.io.IOException;
import java.util.Properties;

public class ProducerThread implements Runnable {

	private final KafkaProducer<Integer, Transaction> producer;
	private final String topic;
	private final Integer producerNumber;
	private final DataGenerator dataGenerator;
	private final Properties props;


	public ProducerThread(int producerNumber) throws IOException {
		this.props = PropertiesHandler.getInstance().getModuleProperties();
		this.producer = new KafkaProducer<Integer, Transaction>(props, new IntegerSerializer(), new TransactionSerializer());
		this.topic = props.getProperty("topic");
		this.producerNumber = producerNumber;
		this.dataGenerator = new DataGenerator(String.valueOf(producerNumber));
	}

	@Override
	public void run() {
		int n_messages = Integer.parseInt(props.getProperty("n_messages_per_producer", "0"));
		for (int i = 0; i < n_messages; i++) {
			Transaction msg;
//            msg = new Transaction(null,"Transaction"+i,"Sender"+producerNumber,"SenderType"+producerNumber,"Receiver"+i,"ReceiverType"+i, 50.0);
			msg = dataGenerator.generateOne();
			producer.send(new ProducerRecord<Integer, Transaction>(topic, producerNumber, msg), new Callback() {
				public void onCompletion(RecordMetadata metadata, Exception e) {
					if (e != null) {
						e.printStackTrace();
					}
					System.out.println("Sent transaction " + msg.getTransactionId() + " from "
							+ msg.getSenderId() + " to " + msg.getReceiverId()
							+ ", Partition: " + metadata.partition() + ", Offset: "
							+ metadata.offset());
				}
			});

		}
		// closes producer
		producer.close();

	}


}
