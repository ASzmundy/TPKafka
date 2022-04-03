package Consumers;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.beans.XMLEncoder;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import Objects.*;

public class Cs3 {
    private final static String TOPIC = "Topic3";
    private final static String BOOTSTRAP_SERVERS =
            "localhost:9092";

    final static String tmpdirectoryname = "tmp";
    final static String outputdirectoryName = "xmloutput";



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
                    Logger lgr = Logger.getLogger(Cs1.class.getName());
                    //Action pour chaque message du topic

                    //Cas de l'export
                    if (Objects.equals(record.value().split("\n")[0], "export")){
                        //Création du dossier output
                        File outputdir = new File(outputdirectoryName);
                        if (!outputdir.exists()) outputdir.mkdir();

                        try {
                            int noligne=0;
                            if(record.value().split("\n")[1].contains("country")) { //Si export de la table des pays
                                List<Country> countries = new ArrayList<>();
                                for (String line : record.value().split("\n")) { //Pour chaque ligne du message
                                    if (noligne > 1) {
                                        //Récupération des valeurs
                                        String[] linesplit = line.split("¤"); // à des fins de debug

                                        String country = line.split("¤")[0];
                                        String countrycode = line.split("¤")[1];
                                        String slug = line.split("¤")[2];
                                        int newconfirmed = Integer.parseInt(line.split("¤")[3]);
                                        int totalconfirmed = Integer.parseInt(line.split("¤")[4]);
                                        int newdeaths = Integer.parseInt(line.split("¤")[5]);
                                        int totaldeaths = Integer.parseInt(line.split("¤")[6]);
                                        int newrecovered = Integer.parseInt(line.split("¤")[7]);
                                        int totalrecovered = Integer.parseInt(line.split("¤")[8]);
                                        String datemaj = line.split("¤")[9];

                                        //Création de l'objet country à instancier
                                        Country ctr = new Country(country, countrycode, slug, newconfirmed, totalconfirmed, newdeaths, totaldeaths, newrecovered, totalrecovered, datemaj);

                                        //Ajout de du country à la liste countries
                                        countries.add(ctr);
                                    }
                                    noligne++;
                                }
                                //Export XML de la liste
                                XMLEncoder xmlEncoder = new XMLEncoder(new FileOutputStream(outputdirectoryName + "/countries.xml"));
                                try {
                                    // serialisation de l'objet
                                    xmlEncoder.writeObject(countries);
                                    xmlEncoder.flush();
                                } finally {
                                    // fermeture de l'encodeur
                                    xmlEncoder.close();
                                }
                                lgr.log(Level.INFO, "Table countries exportée avec succès !");
                            }else { //Si export de la table global
                                List<Global> globals = new ArrayList<>();
                                for (String line : record.value().split("\n")) { //Pour chaque ligne du message
                                    if (noligne > 1) {
                                        //Récupération des valeurs

                                        int newconfirmed = Integer.parseInt(line.split("¤")[0]);
                                        int totalconfirmed = Integer.parseInt(line.split("¤")[1]);
                                        int newdeaths = Integer.parseInt(line.split("¤")[2]);
                                        int totaldeaths = Integer.parseInt(line.split("¤")[3]);
                                        int newrecovered = Integer.parseInt(line.split("¤")[4]);
                                        int totalrecovered = Integer.parseInt(line.split("¤")[5]);

                                        String datemaj = line.split("¤")[6];

                                        //Création de l'objet global à instancier
                                        Global ctr = new Global(newconfirmed, totalconfirmed, newdeaths, totaldeaths, newrecovered, totalrecovered, datemaj);

                                        //Ajout de du country à la liste countries
                                        globals.add(ctr);
                                    }
                                    noligne++;
                                }
                                //Export XML de la liste
                                XMLEncoder xmlEncoder = new XMLEncoder(new FileOutputStream(outputdirectoryName + "/global.xml"));
                                try {
                                    // serialisation de l'objet
                                    xmlEncoder.writeObject(globals);
                                    xmlEncoder.flush();
                                } finally {
                                    // fermeture de l'encodeur
                                    xmlEncoder.close();
                                }
                                lgr.log(Level.INFO, "Table global exportée avec succès !");
                            }
                        } catch (FileNotFoundException e) {
                            e.printStackTrace();
                        }
                    }
                    else {
                        //Cas des autres commandes (affichage des résultats dans la console)
                        //Création du dossier temporaire si il n'existe pas
                        File directory = new File(String.valueOf(tmpdirectoryname));
                        if (!directory.exists()) {
                            directory.mkdir();
                        }

                        //Création du fichier temporaire
                        String filepath = tmpdirectoryname + "/" + record.value().split("\n")[0] + ".tmp";
                        File f = new File(filepath);

                        //Insertion des données dans le fichier temporaire
                        try{
                            FileWriter fw = new FileWriter(filepath);
                            BufferedWriter bw = new BufferedWriter(new FileWriter(filepath));
                            int noligne = 0;
                            for (String line : record.value().split("\n")){ //Pour chaque ligne du message
                                switch (noligne){
                                    case 0:
                                        bw.write("Résultats pour "+line+" :");
                                        break;
                                    case 1:
                                        String newline = line.replace("¤", " | ");
                                        bw.write(newline);
                                        bw.write("\n");
                                        for(int i = 0; i < newline.length(); i++){
                                            bw.write("-");
                                        }
                                        break;
                                    default:
                                        bw.write(line.replace("¤", " | "));
                                }
                                bw.write("\n");
                                noligne++;
                            }
                            bw.close();
                            fw.close();
                            lgr.log(Level.INFO, "Fichier temporaire "+filepath+" créé !");
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }

                });
                consumer.commitAsync();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        consumer.close();
        System.out.println("DONE");
    }

    public static void main(String[] args) throws Exception {
        Cs3.runConsumer();
    }
}
