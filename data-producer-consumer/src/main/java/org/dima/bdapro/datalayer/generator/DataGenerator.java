package org.dima.bdapro.datalayer.generator;

import org.dima.bdapro.datalayer.bean.Transaction;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class DataGenerator {

	private static final String TRANSACTION_ID_FORMAT = "%d-%05d";


	private String threadId;
	private int transactionCounter = 0;
	private Resellers resellers;
	private int resellerTypes;


	public DataGenerator(String threadId) throws IOException {
		this.threadId = threadId;
		this.resellers = new Resellers(threadId);
		this.resellerTypes = this.resellers.getTypesSize();

	}

	public Transaction generateOne() {
		Transaction transaction = new Transaction();

		genSenderData(transaction);
		genRecevierData(transaction);

		genTransactionAmount(transaction);
		Long timestamp = System.nanoTime();
		transaction.setTransactionTime(timestamp); // TODO: Generate future data as well?
		genTransactionId(transaction, timestamp);

		return transaction;
	}

	private void genTransactionAmount(Transaction transaction) {
		transaction.setTransactionAmount(Math.abs(ThreadLocalRandom.current().nextDouble()));
	}

	private void genRecevierData(Transaction transaction) {
		int t = ThreadLocalRandom.current().nextInt(0, resellerTypes);

		transaction.setReceiverType(resellers.getTypeName(t));
		transaction.setReceiverId(resellers.getResellerId(0, ThreadLocalRandom.current().nextInt()));
		// TODO: maybe check the receiver is the same as sender or not.

	}

	private void genSenderData(Transaction transaction) {

		int t = ThreadLocalRandom.current().nextInt(0, resellerTypes);

		transaction.setSenderType(resellers.getTypeName(t));
		transaction.setSenderId(resellers.getResellerId(0, ThreadLocalRandom.current().nextInt()));


	}

	private void genTransactionId(Transaction transaction, Long timestamp) {

		String id = String.format(TRANSACTION_ID_FORMAT, timestamp, ++transactionCounter);

		if (transactionCounter >= Integer.MAX_VALUE - 1) {
			transactionCounter = 0;
		}

		transaction.setTransactionId(id);
	}


}
