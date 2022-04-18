package io.tpd.kafkaexample;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaSendCallback;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;

@RestController
@RequiredArgsConstructor
@Slf4j
public class HelloKafkaController {
    @lombok.Value
    @Builder
    @Jacksonized
    private static class MessageDto {
        String message;
    }

    private final KafkaTemplate<String, PracticalAdvice> template;
    private final KafkaProperties kafkaProperties;

    @Value("${tpd.topic-name}")
    private final String topicName;
    @Value("${tpd.messages-per-request}")
    private final int messagesPerRequest;

    private CountDownLatch latch;

    @PostMapping("/test")
    public String test(InputStream is) {
        try {
            byte[] buffer = is.readAllBytes();
            var str = new String(buffer, Charset.defaultCharset());
            log.info("str = {}", str);
        } catch (IOException e) {
            e.printStackTrace();
            return "error";
        }
        return "hello";
    }

    @PostMapping("/hello")
    public String hello(@RequestBody MessageDto messageDto) throws Exception {
        latch = new CountDownLatch(messagesPerRequest);
        var future = this.template.send(
                topicName,
                String.valueOf(1),
                new PracticalAdvice(messageDto.getMessage(), 1));

        future.addCallback(new KafkaSendCallback<>() {
            @Override
            public void onSuccess(SendResult<String, PracticalAdvice> result) {
                log.info("[producer] message sent successfully record {} | metadata {}",
                        result.getProducerRecord(),
                        result.getRecordMetadata());
            }

            @Override
            public void onFailure(KafkaProducerException ex) {
                log.error("[producer] error sending message to queue");
            }
        });
        latch.await(60, TimeUnit.SECONDS);

        log.info("All messages received");
        return "Hello Kafka!";
    }

    @KafkaListener(
            topics = "advice-topic",
            clientIdPrefix = "json-consumer",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void listenAsObject(ConsumerRecord<String, PracticalAdvice> cr,
                               @Payload PracticalAdvice payload) {
        kafkaProperties.getProperties().get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG);

        log.info("Logger 1 [JSON] received key {}: Type [{}] | Payload: {} | Record: {}",
                cr.key(),
                typeIdHeader(cr.headers()),
                payload,
                cr);
        latch.countDown();
    }

/*
    @KafkaListener(
            topics = "advice-topic",
            clientIdPrefix = "string",
            containerFactory = "kafkaListenerStringContainerFactory"
    )
    public void listenasString(ConsumerRecord<String, String> cr,
                               @Payload String payload) {
        logger.info("Logger 2 [String] received key {}: Type [{}] | Payload: {} | Record: {}",
                cr.key(),
                typeIdHeader(cr.headers()),
                payload,
                cr);
        latch.countDown();
    }

    @KafkaListener(
            topics = "advice-topic",
            clientIdPrefix = "bytearray",
            containerFactory = "kafkaListenerByteArrayContainerFactory"
    )
    public void listenAsByteArray(ConsumerRecord<String, byte[]> cr,
                                  @Payload byte[] payload) {
        logger.info("Logger 3 [ByteArray] received key {}: Type [{}] | Payload: {} | Record: {}",
                cr.key(),
                typeIdHeader(cr.headers()),
                payload,
                cr);
        latch.countDown();
    }
*/

    private static String typeIdHeader(Headers headers) {
        return StreamSupport.stream(headers.spliterator(), false)
                .filter(header -> header.key().equals("__TypeId__"))
                .findFirst().map(header -> new String(header.value())).orElse("N/A");
    }
}
