package Consumers;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Cs1 {
    private final static String TOPIC = "Topic1";
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
                "Consumers.Cs1");
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

        final int giveUp = 5000;   int noRecordsCount = 0;

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
                    JSONObject obj = new JSONObject(record.value());

                    //Parse du JSON vers objets java
                    //Parse du global
                    JSONObject global = obj.getJSONObject("Global");
                    Integer globalnewconfirmed = global.getInt("NewConfirmed");
                    Integer globaltotalconfirmed = global.getInt("TotalConfirmed");
                    Integer globalnewdeaths = global.getInt("NewDeaths");
                    Integer globaltotaldeaths = global.getInt("TotalDeaths");
                    Integer globalnewrecovered = global.getInt("NewRecovered");
                    Integer globaltotalrecovered = global.getInt("TotalRecovered");
                    DateFormat df1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                    Date globaldate = null;
                    try {
                        globaldate = df1.parse(global.getString("Date"));
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }


                    //Envoi du global dans la BDD
                    //Génération de l'ébauche de la requête SQL
                    String globalquery =
                        "INSERT INTO \"global\"(" +
                        "NewConfirmed" +
                        ",TotalConfirmed" +
                        ",NewDeaths" +
                        ",TotalDeaths" +
                        ",NewRecovered" +
                        ",TotalRecovered" +
                        ",Datemaj" +
                        ")SELECT " +
                        "?,?,?,?,?,?,? " +
                        "WHERE " +
                            "NOT EXISTS (" +
                                "SELECT Datemaj FROM \"global\" WHERE Datemaj = ?" +
                            ");";
                    //Connection à la BDD et remplissage de la requête SQL
                    try (Connection con = DriverManager.getConnection(dburl, user, password);
                         PreparedStatement pst = con.prepareStatement(globalquery)) {

                        pst.setInt(1, globalnewconfirmed);
                        pst.setInt(2, globaltotalconfirmed);
                        pst.setInt(3, globalnewdeaths);
                        pst.setInt(4, globaltotaldeaths);
                        pst.setInt(5, globalnewrecovered);
                        pst.setInt(6, globaltotalrecovered);
                        assert globaldate != null;
                        Timestamp ts = new Timestamp(globaldate.getTime());
                        pst.setTimestamp(7,ts);
                        pst.setTimestamp(8,ts);
                        pst.executeUpdate();

                        Logger lgr = Logger.getLogger(Cs1.class.getName());
                        lgr.log(Level.INFO, "Ligne inséré dans global !");
                    } catch (SQLException ex) {
                        Logger lgr = Logger.getLogger(Cs1.class.getName());
                        lgr.log(Level.SEVERE, ex.getMessage(), ex);
                    }

                    //Parse des countries
                    JSONArray countries = obj.getJSONArray("Countries");
                    int i;
                    for (i=0 ; i < countries.length(); i++) {
                        JSONObject selectedcountry = countries.getJSONObject(i);

                        String country = selectedcountry.getString("Country");
                        String countrycode = selectedcountry.getString("CountryCode");
                        String slug = selectedcountry.getString("Slug");
                        Integer newconfirmed = selectedcountry.getInt("NewConfirmed");
                        Integer totalconfirmed = selectedcountry.getInt("TotalConfirmed");
                        Integer newdeaths = selectedcountry.getInt("NewDeaths");
                        Integer totaldeaths = selectedcountry.getInt("TotalDeaths");
                        Integer newrecovered = selectedcountry.getInt("NewRecovered");
                        Integer totalrecovered = selectedcountry.getInt("TotalRecovered");

                        Date date = null;
                        try {
                            date = df1.parse(selectedcountry.getString("Date"));
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }


                        //Envoi du country dans la BDD
                        //Génération de l'ébauche de la requête SQL
                        String countryquery =
                                "INSERT INTO \"countries\"(" +
                                        "Country" +
                                        ",CountryCode" +
                                        ",Slug" +
                                        ",NewConfirmed" +
                                        ",TotalConfirmed" +
                                        ",NewDeaths" +
                                        ",TotalDeaths" +
                                        ",NewRecovered" +
                                        ",TotalRecovered" +
                                        ",Datemaj" +
                                        ")SELECT " +
                                        "?,?,?,?,?,?,?,?,?,? " +
                                        "WHERE " +
                                        "NOT EXISTS (" +
                                            "SELECT Datemaj, CountryCode FROM \"countries\" WHERE Datemaj = ? AND CountryCode = ?" +
                                        ");";
                        //Connection à la BDD et remplissage de la requête SQL
                        try (Connection con = DriverManager.getConnection(dburl, user, password);
                             PreparedStatement pst = con.prepareStatement(countryquery)) {

                            pst.setString(1,country);
                            pst.setString(2,countrycode);
                            pst.setString(3,slug);
                            pst.setInt(4, newconfirmed);
                            pst.setInt(5, totalconfirmed);
                            pst.setInt(6, newdeaths);
                            pst.setInt(7, totaldeaths);
                            pst.setInt(8, newrecovered);
                            pst.setInt(9, totalrecovered);
                            assert date != null;
                            Timestamp ts = new Timestamp(date.getTime());
                            pst.setTimestamp(10,ts);
                            pst.setTimestamp(11,ts);
                            pst.setString(12,countrycode);
                            pst.executeUpdate();
                            //Logger lgr = Logger.getLogger(Consumers.Cs1.class.getName());
                            //lgr.log(Level.INFO, "Ligne "+i+" inséré dans countries !");

                        } catch (SQLException ex) {
                            Logger lgr = Logger.getLogger(Cs1.class.getName());
                            lgr.log(Level.SEVERE, ex.getMessage(), ex);
                        }
                    }
                    //Affichage du nom de lignes insérées
                    Logger lgr = Logger.getLogger(Cs1.class.getName());
                    lgr.log(Level.INFO,i + " lignes inséré(s) dans countries !");
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
        Cs1.runConsumer();
    }
}
