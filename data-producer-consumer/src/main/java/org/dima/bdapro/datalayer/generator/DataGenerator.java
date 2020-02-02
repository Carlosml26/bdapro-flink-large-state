package org.dima.bdapro.datalayer.generator;

import org.dima.bdapro.datalayer.bean.Transaction;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import static org.dima.bdapro.utils.Constants.RESELLER_TRANSACTION_PROFILE;
import static org.dima.bdapro.utils.Constants.SUBSCRIBER_TRANSACTION_PROFILE;
import static org.dima.bdapro.utils.Constants.TOPUP_PROFILE;

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

	public Transaction generateCreditTransaction() {
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

	public Transaction generateTopupTransaction() {
		Transaction transaction = new Transaction();

		genSenderDataOfType(transaction, resellers.getTypesSize() - 2); // 2nd last is retail
		genReceiverDataOfType(transaction, resellers.getTypesSize() - 1); // last is subscriber

		genTransactionAmount(transaction);
		Long timestamp = System.currentTimeMillis();
		transaction.setTransactionTime(timestamp); // TODO: Generate future data as well?
		genTransactionId(transaction, timestamp);

		transaction.setProfileId(TOPUP_PROFILE);

		return transaction;
	}

	/**
	 * Money goes back to the top level after the subscriber usage.
	 * @return
	 */
	public Transaction generateCallTransaction() {
		Transaction transaction = new Transaction();

		genSenderDataOfType(transaction, resellers.getTypesSize() - 1); // last is subscriber
		genReceiverDataOfType(transaction, 0); // top-level reseller.

		genTransactionAmount(transaction);
		Long timestamp = System.currentTimeMillis();
		transaction.setTransactionTime(timestamp); // TODO: Generate future data as well?
		genTransactionId(transaction, timestamp);

		transaction.setProfileId(SUBSCRIBER_TRANSACTION_PROFILE);

		return transaction;
	}

	private void genTransactionAmount(Transaction transaction) {
		transaction.setTransactionAmount(Math.abs(ThreadLocalRandom.current().nextDouble()));
	}

	private void genRecevierData(Transaction transaction) {
		int t = ThreadLocalRandom.current().nextInt(0, resellerTypes);
		genReceiverDataOfType(transaction, t);
	}

	private void genSenderData(Transaction transaction) {

		int typeIdIndex = ThreadLocalRandom.current().nextInt(0, resellerTypes);
		genSenderDataOfType(transaction, typeIdIndex);

	}

	private void genSenderDataOfType(Transaction transaction, int typeIdIndex) {
		transaction.setSenderType(resellers.getTypeName(typeIdIndex));
		transaction.setSenderId(resellers.getResellerId(typeIdIndex, ThreadLocalRandom.current().nextInt(0, resellers.getTypeLength(typeIdIndex))));
	}


	private void genReceiverDataOfType(Transaction transaction, int typeIdIndex) {
		transaction.setReceiverType(resellers.getTypeName(typeIdIndex));
		transaction.setReceiverId(resellers.getResellerId(typeIdIndex, ThreadLocalRandom.current().nextInt(0, resellers.getTypeLength(typeIdIndex))));
	}

	private void genTransactionId(Transaction transaction, Long timestamp) {

		String id = String.format(TRANSACTION_ID_FORMAT, threadId, timestamp, ++transactionCounter);

		if (transactionCounter >= Integer.MAX_VALUE - 1) {
			transactionCounter = 0;
		}

		transaction.setTransactionId(id);
	}


	public Transaction genTransaction(int seed, int credit, int topup, int call) {
		int total = credit + topup + call;
		int rem = seed % total;

		if(rem >= 0 && rem < credit)
		{
			return generateCreditTransaction();
		}
		else if (rem >= credit && rem < credit + topup) {
			return generateTopupTransaction();
		}
		else {
			return generateCallTransaction();
		}
	}


}
