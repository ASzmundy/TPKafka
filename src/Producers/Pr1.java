package Producers;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class Pr1 {
    private final static String TOPIC = "Topic1";
    private final static String BOOTSTRAP_SERVERS =
            "http://localhost:9092";

    private static Producer<Long, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put("schema.registry.url","http://localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "Producers.Pr1");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    static void runProducer(final int sendMessageCount) throws Exception {
        final Producer<Long, String> producer = createProducer();
        long time = System.currentTimeMillis();

        while (true) {
            try {
                //Récupération du JSON
                URL url = new URL("https://api.covid19api.com/summary");
                HttpURLConnection con = (HttpURLConnection) url.openConnection();
                con.setRequestMethod("GET");
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(con.getInputStream()));
                String inputLine;
                StringBuffer content = new StringBuffer();
                while ((inputLine = in.readLine()) != null) {
                    content.append(inputLine);
                }
                in.close();

                //Envoi du JSON vers le Topic1
                for (long index = time; index < time + sendMessageCount; index++) {
                    final ProducerRecord<Long, String> record =
                            new ProducerRecord<>(TOPIC, index,
                                    content.toString());

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
            TimeUnit.MINUTES.sleep(30);
        }
    }

    public static void main(String[] args) throws Exception {
        Pr1.runProducer(1);
    }
}
