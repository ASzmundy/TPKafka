package Producers;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

// Ce producer envoie une commande dans le topic 2 à partir de la console Console.Cs
public class Pr2 {
    private final static String TOPIC = "Topic2";
    private final static String BOOTSTRAP_SERVERS =
            "http://localhost:9092";

    private static Producer<Long, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put("schema.registry.url","http://localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "Producers.Pr2");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    public static void runProducer(final int sendMessageCount, final String command) throws Exception {
        final Producer<Long, String> producer = createProducer();
        long time = System.currentTimeMillis();
            try {

                //Envoi de la commande vers le Topic2
                for (long index = time; index < time + sendMessageCount; index++) {
                    final ProducerRecord<Long, String> record =
                            new ProducerRecord<>(TOPIC, index,
                                    command);

                    RecordMetadata metadata = producer.send(record).get();

                    long elapsedTime = System.currentTimeMillis() - time;
                    System.out.printf("sent record(key=%s value=%s) " +
                                    "meta(partition=%d, offset=%d) time=%d\n",
                            record.key(), record.value(), metadata.partition(),
                            metadata.offset(), elapsedTime);
                }
            } finally {
                producer.flush();
                producer.close();
            }
    }
}
