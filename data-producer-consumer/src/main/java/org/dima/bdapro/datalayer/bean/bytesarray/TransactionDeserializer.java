package org.dima.bdapro.datalayer.bean.bytesarray;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Deserializer;
import org.dima.bdapro.datalayer.bean.Transaction;

import java.io.Serializable;

public class TransactionDeserializer implements Deserializer<Transaction>, Serializable {

	@Override
	public Transaction deserialize(String s, byte[] bytes) {
		return SerializationUtils.deserialize(bytes);
	}
}


