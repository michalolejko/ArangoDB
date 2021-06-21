import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.CollectionEntity;
import com.arangodb.mapping.ArangoJack;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Scanner;

//https://www.arangodb.com/download-major/windows/

public class Main {

    static Scanner scanner;
    static String dbName = "baza";
    static String collectionName = "kolekcja";
    static CollectionEntity myArangoCollection;
    static ArangoDB arangoDB;
    static int id = 1;

    public static void main(String[] args) {

        arangoDB = new ArangoDB.Builder()
                .host("127.0.0.1", 8529)
                .serializer(new ArangoJack())
                .build();
        if (arangoDB.db(dbName) == null)
            try {
                arangoDB.createDatabase(dbName);
                System.out.println("Database created: " + dbName);
            } catch (ArangoDBException e) {
                System.err.println("Failed to create database: " + dbName + "; " + e.getMessage());

            }

        arangoDB.db(dbName).getCollections()
                .stream()
                .filter(c -> c.getName() == collectionName)
                .findFirst()
                .orElse(null);

        if (arangoDB.db(dbName).getCollections() == null)
            try {
                myArangoCollection = arangoDB.db(dbName).createCollection(collectionName);
                System.out.println("Collection created: " + myArangoCollection.getName());
            } catch (ArangoDBException e) {
                System.err.println("Failed to create collection: " + collectionName + "; " + e.getMessage());
            }


        scanner = new Scanner(System.in);
        while (true) {
            showAllRecords();
            pressEnter();
            showMenu();
            switch (scanner.nextInt()) {
                case 0:
                    System.out.println("Zakonczono");
                    return;
                case 1:
                    saveRecord();
                    break;
                case 2:
                    updateRecord();
                    break;
                case 3:
                    deleteRecord();
                    break;
                case 4:
                    getRecordById();
                    break;
                case 5:
                    gerRecordByStatement();
                    break;
                case 6:
                    processing();
                    break;
            }
        }
    }


    private static void processing() {
        for (int i = 1; i < id; i++)
            try {
                BaseDocument myDocument = arangoDB.db(dbName).collection(collectionName).getDocument(Integer.toString(i), BaseDocument.class);
                if(myDocument == null)
                    continue;
                int newValue = (Integer.parseInt(myDocument.getAttribute("value").toString())+100);
                myDocument.updateAttribute("value", newValue);
                arangoDB.db(dbName).collection(collectionName).updateDocument(Integer.toString(i), myDocument);
                printRecord(myDocument);

            } catch (ArangoDBException e) {
                System.err.println("ID " + i + " - nothing.");
            }
        System.out.println();
    }

    private static void gerRecordByStatement() {
        System.out.println("MANDATY POWYZEJ 300 ZLOTYCH: ");
        for (int i = 1; i < id; i++)
            try {
                BaseDocument myDocument = arangoDB.db(dbName).collection(collectionName).getDocument(Integer.toString(i), BaseDocument.class);
                if(myDocument==null)
                    continue;
                if (Integer.parseInt(myDocument.getAttribute("value").toString()) > 300)
                    printRecord(myDocument);
            } catch (ArangoDBException e) {
                System.out.println("ID " + i + " - nothing.");
            }
        System.out.println();
    }

    private static void getRecordById() {
        System.out.println("PODAJ ID MANDATU KTORY CHCESZ POBRAC:");
        int i = scanner.nextInt();
        try {
            BaseDocument myDocument = arangoDB.db(dbName).collection(collectionName).getDocument(Integer.toString(i), BaseDocument.class);
            printRecord(myDocument);
        } catch (ArangoDBException e) {
            System.err.println("ID " + i + " - nothing.");
        }
    }

    private static void deleteRecord() {
        showAllRecords();
        System.out.println("Podaj id do aktualizacji: ");
        int id = scanner.nextInt();
        try {
            arangoDB.db(dbName).collection(collectionName).deleteDocument(Integer.toString(id));
            System.out.println("Usunieto.");
        } catch (ArangoDBException e) {
            System.err.println("Failed to delete document. " + e.getMessage());
        }
    }

    private static void updateRecord() {
        showAllRecords();
        System.out.println("Podaj id do aktualizacji: ");
        int id = scanner.nextInt();
        scanner.nextLine();
        System.out.println("Podaj zaktualizowana wysokosc mandatu: ");
        String newValue = scanner.nextLine();
        try {
            BaseDocument myUpdatedDocument = arangoDB.db(dbName).collection(collectionName).getDocument(Integer.toString(id), BaseDocument.class);
            myUpdatedDocument.updateAttribute("value", newValue);
            arangoDB.db(dbName).collection(collectionName).updateDocument(Integer.toString(id), myUpdatedDocument);
            printRecord(myUpdatedDocument);
        } catch (ArangoDBException e) {
            System.err.println("Failed to get document: myKey; " + e.getMessage());
        }
    }

    private static void saveRecord() {
        scanner.nextLine();
        System.out.println("Podaj rejestracje: ");
        String reg = scanner.nextLine().toUpperCase();
        System.out.println("Podaj wysokosc mandatu: ");
        String ticketValue = scanner.nextLine();
        String strDate = new SimpleDateFormat("yyyy-mm-dd").format(Calendar.getInstance().getTime());
        BaseDocument myObject = new BaseDocument();
        myObject.setKey(Integer.toString(id++));
        myObject.addAttribute("register", reg);
        myObject.addAttribute("value", ticketValue);
        myObject.addAttribute("date", strDate);
        int breakLoop = 0;
        while (true) {
            if (breakLoop > 300)
                break;
            try {
                arangoDB.db(dbName).collection(collectionName).insertDocument(myObject);
                System.out.print("Document created: ");
                printRecord(myObject);
                System.out.println();
                break;
            } catch (ArangoDBException e) {
                System.err.println("Trying with another id...");
                breakLoop++;
                myObject.setKey(Integer.toString(++id));
                id++;
            }
        }
    }

    private static void showAllRecords() {
        System.out.println("WSZYSTKIE MANDATY:");
        for (int i = 1; i < id; i++)
            try {
                BaseDocument myDocument = arangoDB.db(dbName).collection(collectionName).getDocument(Integer.toString(i), BaseDocument.class);
                printRecord(myDocument);
            } catch (ArangoDBException e) {
                System.err.println("ID " + i + " - nothing.");
            }
    }


    private static void pressEnter() {
        System.out.println("Wcisnij enter, aby kontynuowac...");
        try {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void showMenu() {
        System.out.print("\n2) Policja (Amazon Neptune))\n\nWybierz operacje:\n" +
                "1.Zapisywanie\n2.Aktualizowanie\n3.Kasowanie\n4.Pobieranie mandatu po id\n5.Pobieranie mandatow powyzej 300zl\n" +
                "6.Przetwarzanie(wszytskie mandaty += 100zł)\n0.Zakoncz\n\nWpisz cyfre i zatwierdz enterem: ");
    }

    private static void printRecord(BaseDocument myDocument) {
        if(myDocument==null)
            return;
        System.out.print("ID: " + myDocument.getKey());
        System.out.print(", REGISTER: " + myDocument.getAttribute("register"));
        System.out.print(", VALUE: " + myDocument.getAttribute("value") + " PLN");
        System.out.println(", DATE: " + myDocument.getAttribute("date"));
    }
}
