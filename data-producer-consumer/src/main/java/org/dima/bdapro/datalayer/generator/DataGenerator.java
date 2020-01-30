package org.dima.bdapro.datalayer.generator;

import org.dima.bdapro.datalayer.bean.Transaction;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import static org.dima.bdapro.utils.Constants.RESELLER_TRANSACTION_PROFILE;

public class DataGenerator {

	private static final String TRANSACTION_ID_FORMAT = "%s.%d.%05d";


	private String threadId;
	private int transactionCounter = 0;
	private Resellers resellers;
	private int resellerTypes;


	public DataGenerator(String threadId) throws IOException {
		this.threadId = threadId;
		this.resellers = new Resellers(threadId);
		this.resellerTypes = this.resellers.getTypesSize();

	}

	public Transaction generateResellerTransaction() {
		Transaction transaction = new Transaction();

		genSenderData(transaction);
		genRecevierData(transaction);

		genTransactionAmount(transaction);
		Long timestamp = System.currentTimeMillis();
		transaction.setTransactionTime(timestamp); // TODO: Generate future data as well?
		genTransactionId(transaction, timestamp);

		transaction.setProfileId(RESELLER_TRANSACTION_PROFILE);

		return transaction;
	}

	private void genTransactionAmount(Transaction transaction) {
		transaction.setTransactionAmount(Math.abs(ThreadLocalRandom.current().nextDouble()));
	}

	private void genRecevierData(Transaction transaction) {
		int t = ThreadLocalRandom.current().nextInt(0, resellerTypes);

		transaction.setReceiverType(resellers.getTypeName(t));
		transaction.setReceiverId(resellers.getResellerId(t, ThreadLocalRandom.current().nextInt(0, resellers.getTypeLength(t))));// TODO: maybe check the receiver is the same as sender or not.

	}

	private void genSenderData(Transaction transaction) {

		int t = ThreadLocalRandom.current().nextInt(0, resellerTypes);

		transaction.setSenderType(resellers.getTypeName(t));
		transaction.setSenderId(resellers.getResellerId(t, ThreadLocalRandom.current().nextInt(0, resellers.getTypeLength(t))));


	}

	private void genTransactionId(Transaction transaction, Long timestamp) {

		String id = String.format(TRANSACTION_ID_FORMAT, threadId, timestamp, ++transactionCounter);

		if (transactionCounter >= Integer.MAX_VALUE - 1) {
			transactionCounter = 0;
		}

		transaction.setTransactionId(id);
	}


}
