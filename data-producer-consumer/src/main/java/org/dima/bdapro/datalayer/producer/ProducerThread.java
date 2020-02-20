package org.dima.bdapro.datalayer.producer;

import com.google.common.util.concurrent.RateLimiter;
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
import java.util.concurrent.ThreadLocalRandom;

public class ProducerThread implements Runnable {

	private final KafkaProducer<Integer, Transaction> producer;
	private final String topic;
	private final Integer producerNumber;
	private final DataGenerator dataGenerator;
	private final Properties props;
	private final int maxMessagesPerSecond;


	public ProducerThread(int producerNumber, int maxMessagesPerSecond) throws IOException {
		this.props = PropertiesHandler.getInstance().getModuleProperties();
		this.maxMessagesPerSecond = maxMessagesPerSecond;
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
		int i = 0;

		while (true) {
			Transaction msg;
//            msg = new Transaction(null,"Transaction"+i,"Sender"+producerNumber,"SenderType"+producerNumber,"Receiver"+i,"ReceiverType"+i, 50.0);
			msg = dataGenerator.genTransaction(i, p_credit, p_topup, p_call);
			rateLimiter.acquire();

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

			try {//- [TODO]: Remove it for experimentation.
				Thread.sleep(ThreadLocalRandom.current().nextLong(1, 100));
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
			i++;
		}
	}


}
