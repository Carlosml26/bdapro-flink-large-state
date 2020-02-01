package org.dima.bdapro.flink.datalayer.json;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.dima.bdapro.datalayer.bean.Transaction;
import org.dima.bdapro.datalayer.bean.json.TransactionDeserializer;

import java.io.IOException;

public class TransactionDeserializationSchema implements DeserializationSchema<Transaction> {
	private TransactionDeserializer<Transaction> deserializer = new TransactionDeserializer<>(Transaction.class);

	@Override
	public Transaction deserialize(byte[] message) throws IOException {
		return (Transaction) deserializer.deserialize(null, message);
	}

	@Override
	public boolean isEndOfStream(Transaction nextElement) {
		return false;
	}

	@Override
	public TypeInformation<Transaction> getProducedType() {
		return TypeInformation.of(Transaction.class);
	}
}
