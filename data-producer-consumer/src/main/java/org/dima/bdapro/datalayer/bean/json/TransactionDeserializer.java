package org.dima.bdapro.datalayer.bean.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.Map;

public class TransactionDeserializer <T> implements Deserializer, Serializable {

    private Logger logger = LogManager.getLogger(this.getClass());

    private Class <T> type;

    public TransactionDeserializer(Class<T> type) {
        this.type = type;
    }

    @Override
    public void configure(Map map, boolean b) {

    }

    @Override
    public Object deserialize(String s, byte[] bytes) {
        ObjectMapper mapper = new ObjectMapper();
        T obj = null;
        try {
            obj = mapper.readValue(bytes, type);
        } catch (Exception e) {

            logger.error(e.getMessage());
        }
        return obj;
    }

    @Override
    public void close() {

    }
}


