package Consumers;

import Producers.Pr3;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Cs2 {
    private final static String TOPIC = "Topic2";
    private final static String BOOTSTRAP_SERVERS =
            "localhost:9092";
    private final static String dburl = "jdbc:postgresql://localhost:5432/covid19";
    private final static String user = "postgres";
    private final static String password = "admin";

    private static Consumer<Long, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "Consumers.Cs2");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        // Create the consumer using props.
        final Consumer<Long, String> consumer =
                new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }

    static void runConsumer() throws InterruptedException {
        final Consumer<Long, String> consumer = createConsumer();

        final int giveUp = 10000;   int noRecordsCount = 0;

        while (true) {
            try {
                final ConsumerRecords<Long, String> consumerRecords =
                        consumer.poll(1000);

                if (consumerRecords.count() == 0) {
                    noRecordsCount++;
                    if (noRecordsCount > giveUp) break;
                    else continue;
                }

                consumerRecords.forEach(record -> {
                    //Action pour chaque message du topic

                    //Récupère la commande et ses paramètres
                    String command = record.value().split(" ")[0];
                    List<String> parameters = new ArrayList<>(Arrays.asList(record.value().split(" ")));
                    if(record.value().split(" ").length > 1) {
                        parameters.remove(0); //Isole les paramètres
                    }

                    //Transforme l'action de la commande en requête SQL
                    ArrayList<String> requests = new ArrayList<>();
                    switch (command) {
                        case "get_global_values": requests.add("SELECT * " +
                                "FROM \"global\" " +
                                "ORDER BY datemaj DESC " +
                                "LIMIT 1;"
                            );
                        break;
                        case "get_country_values":
                            requests.add("SELECT * " +
                                "FROM \"countries\" " +
                                "WHERE country = '" +
                                parameters.get(0) +
                                "' ORDER BY datemaj DESC " +
                                "LIMIT 1;"
                            );
                            break;
                        case "get_confirmed_avg":
                            requests.add(
                                    "SELECT country,totalconfirmed INTO lastcountriestotalconfirmed FROM \"countries\" WHERE datemaj = (" +
                                    "SELECT datemaj FROM \"countries\" ORDER BY datemaj DESC LIMIT 1" +
                                    ");"
                            );
                            requests.add(
                                    "SELECT SUM(totalconfirmed)/COUNT(country) AS confirmedavg FROM lastcountriestotalconfirmed;"
                            );
                            requests.add(
                                    "DROP TABLE lastcountriestotalconfirmed;"
                            );
                            break;
                        case "get_deaths_avg":
                            requests.add(
                                    "SELECT country,totaldeaths INTO lastcountriestotaldeaths FROM \"countries\" WHERE datemaj = (" +
                                    "SELECT datemaj FROM \"countries\" ORDER BY datemaj DESC LIMIT 1" +
                                    ");"
                            );
                            requests.add(
                                    "SELECT SUM(totaldeaths)/COUNT(country) AS deathsavg FROM lastcountriestotaldeaths;"
                            );
                            requests.add(
                                    "DROP TABLE lastcountriestotaldeaths;"
                            );
                            break;
                        case "get_countries_deaths_percent":
                            requests.add(
                                    "SELECT country,totalconfirmed,totaldeaths INTO lastcountriestotalconfirmedtotaldeaths FROM \"countries\" WHERE datemaj = (" +
                                    "SELECT datemaj FROM \"countries\" ORDER BY datemaj DESC LIMIT 1" +
                                    ");"
                            );
                            requests.add(
                                    "SELECT country, (totaldeaths::float/totalconfirmed)*100 AS deathpercent FROM lastcountriestotalconfirmedtotaldeaths;"
                            );
                            requests.add(
                                    "DROP TABLE lastcountriestotalconfirmedtotaldeaths;"
                            );
                            break;
                        case "export":
                            requests.add("SELECT * FROM \"countries\";");
                            requests.add("SELECT * FROM \"global\";");
                            break;
                    }
                    //Envoi de la ou des requête(s) à la BDD et récupération des résultats
                    try{
                        Connection con = DriverManager.getConnection(dburl, user, password);
                        for (String request:
                             requests) {
                            Statement stmt = con.createStatement();
                            ResultSet rs;
                            if (request.contains(" INTO ") || request.contains("DROP ")) //Si la requête est une requête sans retour
                                stmt.executeUpdate(request);
                            else {
                                rs = stmt.executeQuery(request);
                                //Conversion de la donnée sous format CSV avec délimiteur ¤
                                StringBuilder sb = new StringBuilder();
                                sb.append(command).append("\n");
                                //Contruction du header du fichier et ajout des données
                                switch (command) {
                                    case "get_global_values":
                                        sb.append("newconfirmed")
                                                .append("¤totalconfirmed")
                                                .append("¤newdeaths")
                                                .append("¤totaldeaths")
                                                .append("¤newrecovered")
                                                .append("¤totalrecovered")
                                                .append("¤datemaj\n");
                                        while (rs.next()) {
                                            sb.append(rs.getString("newconfirmed"))
                                                    .append("¤")
                                                    .append(rs.getString("totalconfirmed"))
                                                    .append("¤")
                                                    .append(rs.getString("newdeaths"))
                                                    .append("¤")
                                                    .append(rs.getString("totaldeaths"))
                                                    .append("¤")
                                                    .append(rs.getString("newrecovered"))
                                                    .append("¤")
                                                    .append(rs.getString("totalrecovered"))
                                                    .append("¤")
                                                    .append(rs.getString("datemaj"))
                                                    .append("\n");
                                        }
                                        break;
                                    case "get_country_values":
                                        sb.append("country")
                                                .append("¤countrycode")
                                                .append("¤slug")
                                                .append("¤newconfirmed")
                                                .append("¤totalconfirmed")
                                                .append("¤newdeaths")
                                                .append("¤totaldeaths")
                                                .append("¤newrecovered")
                                                .append("¤totalrecovered")
                                                .append("¤datemaj\n");
                                        while (rs.next()) {
                                            sb.append(rs.getString("country"))
                                                    .append("¤")
                                                    .append(rs.getString("countrycode"))
                                                    .append("¤")
                                                    .append(rs.getString("slug"))
                                                    .append("¤")
                                                    .append(rs.getString("newconfirmed"))
                                                    .append("¤")
                                                    .append(rs.getString("totalconfirmed"))
                                                    .append("¤")
                                                    .append(rs.getString("newdeaths"))
                                                    .append("¤")
                                                    .append(rs.getString("totaldeaths"))
                                                    .append("¤")
                                                    .append(rs.getString("newrecovered"))
                                                    .append("¤")
                                                    .append(rs.getString("totalrecovered"))
                                                    .append("¤")
                                                    .append(rs.getString("datemaj"))
                                                    .append("\n");
                                        }
                                        break;
                                    case "get_confirmed_avg":
                                        sb.append("confirmedavg\n");
                                        while (rs.next()) {
                                            sb.append(rs.getString("confirmedavg"))
                                                    .append("\n");
                                        }
                                        break;
                                    case "get_deaths_avg":
                                        sb.append("deathsavg\n");
                                        while (rs.next()) {
                                            sb.append(rs.getString("deathsavg"))
                                                    .append("\n");
                                        }
                                        break;
                                    case "get_countries_deaths_percent":
                                        sb.append("country");
                                        sb.append("¤deathpercent\n");
                                        while (rs.next()) {
                                            sb.append(rs.getString("country"))
                                                    .append("¤")
                                                    .append(rs.getString("deathpercent"))
                                                    .append("\n");
                                        }
                                        break;
                                    case "export":
                                        if (request.contains("global")) {
                                            sb.append("newconfirmed")
                                                    .append("¤totalconfirmed")
                                                    .append("¤newdeaths")
                                                    .append("¤totaldeaths")
                                                    .append("¤newrecovered")
                                                    .append("¤totalrecovered")
                                                    .append("¤datemaj\n");
                                            while (rs.next()) {
                                                sb.append(rs.getString("newconfirmed"))
                                                        .append("¤")
                                                        .append(rs.getString("totalconfirmed"))
                                                        .append("¤")
                                                        .append(rs.getString("newdeaths"))
                                                        .append("¤")
                                                        .append(rs.getString("totaldeaths"))
                                                        .append("¤")
                                                        .append(rs.getString("newrecovered"))
                                                        .append("¤")
                                                        .append(rs.getString("totalrecovered"))
                                                        .append("¤")
                                                        .append(rs.getString("datemaj"))
                                                        .append("\n");
                                            }
                                        } else if (request.contains("countries")) {
                                            sb.append("country")
                                                    .append("¤countrycode")
                                                    .append("¤slug")
                                                    .append("¤newconfirmed")
                                                    .append("¤totalconfirmed")
                                                    .append("¤newdeaths")
                                                    .append("¤totaldeaths")
                                                    .append("¤newrecovered")
                                                    .append("¤totalrecovered")
                                                    .append("¤datemaj\n");
                                            while (rs.next()) {
                                                sb.append(rs.getString("country"))
                                                        .append("¤")
                                                        .append(rs.getString("countrycode"))
                                                        .append("¤")
                                                        .append(rs.getString("slug"))
                                                        .append("¤")
                                                        .append(rs.getString("newconfirmed"))
                                                        .append("¤")
                                                        .append(rs.getString("totalconfirmed"))
                                                        .append("¤")
                                                        .append(rs.getString("newdeaths"))
                                                        .append("¤")
                                                        .append(rs.getString("totaldeaths"))
                                                        .append("¤")
                                                        .append(rs.getString("newrecovered"))
                                                        .append("¤")
                                                        .append(rs.getString("totalrecovered"))
                                                        .append("¤")
                                                        .append(rs.getString("datemaj"))
                                                        .append("\n");
                                            }
                                        }
                                        break;
                                }
                                //Envoi de la chaîne CSV au producer3 pour envoi dans topic 3
                                Pr3.runProducer(1, sb.toString());
                            }
                        }
                        con.close();
                    } catch (SQLException ex) {
                        Logger lgr = Logger.getLogger(Cs2.class.getName());
                        lgr.log(Level.SEVERE, ex.getMessage(), ex);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                consumer.commitAsync();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        consumer.close();
        System.out.println("DONE");
    }

    public static void main(String[] args) throws Exception {
        Cs2.runConsumer();
    }
}