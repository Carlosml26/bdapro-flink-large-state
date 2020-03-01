package org.dima.bdapro.datalayer.producer;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.dima.bdapro.datalayer.bean.Transaction;
import org.dima.bdapro.datalayer.bean.bytesarray.TransactionSerializer;
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
	private final int maxMessagesPerSecond;
	private final int numberOfMessages;


	public ProducerThread(int producerNumber, int maxMessagesPerSecond, int numberOfMessages) throws IOException {
		this.props = PropertiesHandler.getInstance().getModuleProperties();
		this.maxMessagesPerSecond = maxMessagesPerSecond;
		this.numberOfMessages = numberOfMessages;
		this.producer = new KafkaProducer<Integer, Transaction>(props, new IntegerSerializer(), new TransactionSerializer());
		this.topic = props.getProperty("topic");
		this.producerNumber = producerNumber;
		this.dataGenerator = new DataGenerator(String.valueOf(producerNumber));
	}

	@Override
	public void run() {
		int p_credit = Integer.parseInt(props.getProperty("datagenerator.transaction.p_credit", "1"));
		int p_topup = Integer.parseInt(props.getProperty("datagenerator.transaction.p_topup", "1"));
		int p_call = Integer.parseInt(props.getProperty("datagenerator.transaction.p_call", "1"));
		final RateLimiter rateLimiter = RateLimiter.create(maxMessagesPerSecond);

		for (int j = 0; j < numberOfMessages; j++) {
			Transaction msg;
			msg = dataGenerator.genTransaction(j, p_credit, p_topup, p_call);
			if (msg == null) {
				throw new RuntimeException("The proportion of the transactions is not correct");
			}
			rateLimiter.acquire();

			producer.send(new ProducerRecord<Integer, Transaction>(topic, producerNumber, msg));
		}
	}


}
