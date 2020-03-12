package org.dima.bdapro.datalayer.bean.bytesarray;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Serializer;
import org.dima.bdapro.datalayer.bean.Transaction;

public class TransactionSerializer implements Serializer<Transaction> {

	@Override
	public byte[] serialize(String topic, Transaction data) {
		return SerializationUtils.serialize(data);
	}

}
