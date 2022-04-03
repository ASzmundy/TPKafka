package Console;

import Producers.Pr2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class Cs {
    //Couleurs
    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_BLACK = "\u001B[30m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_WHITE = "\u001B[37m";

    final static String directoryName = "tmp";


    public static List<String> commandList = Arrays.asList("get_global_values","get_country_values","get_confirmed_avg","get_deaths_avg","get_countries_deaths_percent","help","export");
    public static List<String> commandWithParameterList = Arrays.asList("get_country_values");
    public static String helpcontent = "COMMANDES :\n\n" +
            "get_global_values\t\t\tRenvoie les données globales\n\n" + //OK
            "get_country_values [Nom du pays]\t\t\tRenvoie les données d'un pays\n\n" + //OK
            "get_confirmed_avg\t\t\tRenvoie la moyenne des cas confirmés\n\n" + //OK
            "get_deaths_avg\t\t\tRenvoie la moyenne des décès\n\n" + //OK
            "get_countries_deaths_percent\t\t\tRenvoie le pourcentage des décès par rapport aux cas confirmés par pays\n\n" + //OK
            "export\t\t\tProcède à l'export de la base sous format XML pour Mirth Connect\n\n" + //OK
            "help\t\t\tAffiche cette aide\n\n"; // OK


    public static void main(String[] args) throws Exception {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println("Entrer commande :");
            String command = scanner.nextLine();

            //Si contient une commande et le début de la commande correspond à une commande existante
            if (command.isEmpty()) break;

            if (commandList.contains(command.split(" ")[0].toLowerCase())) {
                if (command.toLowerCase().equals("help")) {
                    System.out.println(helpcontent);
                } else if(commandWithParameterList.contains(command.split(" ")[0].toLowerCase()) && command.split(" ").length < 1) { //Si paramètre manquant ou en trop
                    System.out.println(ANSI_RED + "Paramètres invalides !" + ANSI_RESET);
                } else {
                    Pr2.runProducer(1, command);
                    System.out.println(ANSI_GREEN + "Message envoyé dans le topic !" + ANSI_RESET);
                    if(!command.equals("export")){
                        //Récupération des résultats à afficher dans la console
                        File directory = new File(String.valueOf(directoryName));

                        //Attente de création du dossier
                        while(!directory.exists()){
                            TimeUnit.MILLISECONDS.sleep(100);
                        }

                        String filepath = directoryName + "/" + command.split(" ")[0] + ".tmp";
                        File f = new File(filepath);
                        //Attente de création du fichier
                        while(!f.exists()){
                            TimeUnit.SECONDS.sleep(1);
                        }

                        BufferedReader br = new BufferedReader(new FileReader(f));
                        String line = br.readLine();
                        while(line != null){
                            System.out.println(line);
                            line = br.readLine();
                        }
                        br.close();
                        TimeUnit.MILLISECONDS.sleep(50);
                        f.delete();
                        System.out.println("\n\n");
                    }
                }
            }
        }
    }

}