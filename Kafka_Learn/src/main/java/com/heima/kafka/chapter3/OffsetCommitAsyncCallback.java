package com.heima.kafka.chapter3;

import com.heima.kafka.ConsumerClientConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 异步提交
 */
@Slf4j
public class OffsetCommitAsyncCallback extends ConsumerClientConfig {

    private static AtomicBoolean running = new AtomicBoolean(true);

    public static void main(String[] args) {
        Properties props = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    //do some logical processing.
                }
                // 异步回调
                consumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets,
                                           Exception exception) {
                        if (exception == null) {
                            System.out.println(offsets);
                        } else {
                            log.error("fail to commit offsets {}", offsets, exception);
                        }
                    }
                });
            }
        } finally {
            consumer.close();
        }

        try {
            while (running.get()) {
                consumer.commitAsync();
            }
        } finally {
            try {
                consumer.commitSync();
            } finally {
                consumer.close();
            }
        }
    }
}
