package com.heima.kafka.chapter3;

import com.heima.kafka.chapter2.Company;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;


public class ProtostuffDeserializer implements Deserializer<Company> {
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    public Company deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        Schema schema = RuntimeSchema.getSchema(Company.class);
        Company ans = new Company();
        ProtostuffIOUtil.mergeFrom(data, ans, schema);
        return ans;
    }

    public void close() {

    }
}
