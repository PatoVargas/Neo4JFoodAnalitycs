/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package neo4j;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Pattern;
import static neo4j.Neo4J.Relationships.CADENA_RELATIONS;
import static neo4j.Neo4J.Relationships.USER_RELATIONS;
import static neo4j.Neo4J.Usuarios.CADENA;
import static neo4j.Neo4J.Usuarios.USUARIO;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;

/**
 *
 * @author patricio
 */
public class Tweet {
    
    //funcion para traer todos los jsons con los tweets
    public File[] extractTweets() throws IOException{
        File folder = new File("/home/patricio/Escritorio/Neo4J/jsons");
        File[] listOfFiles = folder.listFiles();

        return listOfFiles;
    }
    
    
    //funcion para encontrar todos los usuarios
    public ResourceIterator<Node> allUser(GraphDatabaseService db){
        try (Transaction tx = db.beginTx()) {
                ResourceIterator<Node> aux = db.findNodes(USUARIO);
                tx.success();
                return aux;
            }
        }
    
    public ResourceIterator<Node> allCadenas(GraphDatabaseService db){
        try (Transaction tx = db.beginTx()) {
                ResourceIterator<Node> aux = db.findNodes(CADENA);
                tx.success();
                return aux;
            }
        }
    
    public ResourceIterable<Relationship> allRelations(GraphDatabaseService db){
        try (Transaction tx = db.beginTx()){
            ResourceIterable<Relationship> aux = db.getAllRelationships();
            tx.success();
            return aux;
        }
    }
    
    //leer tweets y crear user si no existen
    public void readTweet(File [] listOfFiles, GraphDatabaseService db) throws IOException, ParseException{
        JSONParser parser = new JSONParser();
                
        for (int i = 0; i < listOfFiles.length; i++) {
            ResourceIterator<Node> existUsers = allUser(db);
            File file = listOfFiles[i];
            if (file.isFile() && file.getName().endsWith(".json")) {
                Object obj = parser.parse(new FileReader(file));
                 
                JSONObject jsonObject = (JSONObject) obj;
                
                String screen_name = (String) jsonObject.get("screen_name");
                long followers_amount = (long) jsonObject.get("followers_amount");
                long follows_amount = (long) jsonObject.get("follows_amount");
                String mentions = (String) jsonObject.get("mentions");
                String hashtags = (String) jsonObject.get("hashtags");
                try (Transaction tx = db.beginTx()) {
                    int create = 0;
                    while(existUsers.hasNext()){
                        Node user = existUsers.next();
                        if(screen_name.equals(user.getProperty("screen_name"))){
                            System.out.println("Ya existe");
                            create = 1;
                            break;
                        }
                    }
                    if(create == 0){
                        db.schema()
                            .constraintFor(Neo4J.Usuarios.USUARIO)
                            .assertPropertyIsUnique("screen_name");
                        db.schema()
                            .constraintFor(Neo4J.Usuarios.CADENA)
                            .assertPropertyIsUnique("screen_name");
                        Node javaNode = db.createNode(Neo4J.Usuarios.USUARIO);
                        javaNode.setProperty("screen_name", screen_name);
                        javaNode.setProperty("followers_amount", followers_amount);
                        javaNode.setProperty("follows_amount", follows_amount);
                        System.out.println("Creado");
                        
                        tx.success();
                    }
                } 
            }
        }
    }
    
    //Crear user mencionados
    public void createUserMentions(File [] listOfFiles, GraphDatabaseService db) throws IOException, ParseException{
        JSONParser parser = new JSONParser();
        
        ArrayList<String> hashtags = new ArrayList<String>();
        hashtags.add("mcdonald");
        hashtags.add("kfc");
        hashtags.add("burgerking");
        hashtags.add("pizzahut");
        hashtags.add("tacobell");
        hashtags.add("wendy's"); 
        hashtags.add("McDonald's");
        
        for (int i = 0; i < listOfFiles.length; i++) {
            File file = listOfFiles[i];
            if (file.isFile() && file.getName().endsWith(".json")) {
                Object obj = parser.parse(new FileReader(file));
                 
                JSONObject jsonObject = (JSONObject) obj;
                
                String screen_name = (String) jsonObject.get("screen_name");
                long followers_amount = (long) jsonObject.get("followers_amount");
                long follows_amount = (long) jsonObject.get("follows_amount");
                String mentions = (String) jsonObject.get("mentions");
                String hashtags_tweet = (String) jsonObject.get("hashtags");
                
                
                Pattern pattern = Pattern.compile(Pattern.quote(";"));
                String[] data = pattern.split(mentions);
                ArrayList<String> sinArroa = new ArrayList<String>();
                char firstLetter;
                char lastLetter;
                for(String dato: data){
                    firstLetter = dato.charAt(0);
                    lastLetter = dato.charAt(dato.length()-1);
                    if(firstLetter == '@'){
                        dato = dato.substring(1);
                    }
                    if(lastLetter == ':'){
                        dato = dato.substring(0,dato.length()-1);
                    }
                    sinArroa.add(dato);
                }
                
                String[] hashtag_filt = pattern.split(hashtags_tweet);
                ArrayList<String> sinGato = new ArrayList<String>();
                for(String dato: hashtag_filt){
                    firstLetter = dato.charAt(0);
                    if(firstLetter == '#'){
                        dato = dato.substring(1);
                    }
                    sinGato.add(dato);
                }
                
                //System.out.println(sinGato);
                
                for(String dato: sinArroa){
                    ResourceIterator<Node> existUsers = allUser(db);
                    try (Transaction tx = db.beginTx()) {
                        int create = 0;
                        while(existUsers.hasNext()){
                            Node user = existUsers.next();
                            if(dato.equals(user.getProperty("screen_name")) || dato.equals("null")){
                                System.out.println("Ya existe");
                                create = 1;
                                break;
                            }
                        }
                        if(create == 0){
                            db.schema()
                                .constraintFor(Neo4J.Usuarios.USUARIO)
                                .assertPropertyIsUnique("screen_name");
                            db.schema()
                                .constraintFor(Neo4J.Usuarios.CADENA)
                                .assertPropertyIsUnique("screen_name");
                            Node javaNode = db.createNode(Neo4J.Usuarios.USUARIO);
                            javaNode.setProperty("screen_name", dato);
                            System.out.println("Creado");
                            tx.success();
                        }
                    }
                }
                
                for(String dato: sinArroa){
                    System.out.println("Creando Relacion");
                    relations(listOfFiles, db, screen_name, dato);
                }        
                
                for(String dato: sinGato){
                    ResourceIterator<Node> existCadenas = allCadenas(db);
                    for(String hash: hashtags){ 
                        if(dato.equalsIgnoreCase(hash)){
                            try (Transaction tx = db.beginTx()) {
                                int create = 0;
                                while(existCadenas.hasNext()){
                                    Node user = existCadenas.next();
                                    if(dato.equals(user.getProperty("screen_name")) || dato.equals("null")){
                                        System.out.println("Ya existe");
                                        create = 1;
                                        break;
                                    }
                                }
                                if(create == 0){
                                    Node javaNode = db.createNode(Neo4J.Usuarios.CADENA);
                                    javaNode.setProperty("screen_name", dato);
                                    System.out.println("Creado");
                                    tx.success();
                                }
                            }
                        }
                    }
                }
                
                for(String dato: sinGato){
                    for(String hash : hashtags){
                        if(dato.equalsIgnoreCase(hash)){
                            System.out.println("Creando Relacion");
                            relations(listOfFiles, db, screen_name, dato);
                        }
                    }
                }                 
            }
        }
    } 
    
    //Crear Relaciones
    public void relations(File [] listOfFiles, GraphDatabaseService db, String usuario, String mencionado) throws IOException, ParseException{
        JSONParser parser = new JSONParser();
        
        ResourceIterator<Node> existUsers = allUser(db);
        ResourceIterator<Node> existMentionUser = allUser(db);
        ResourceIterator<Node> existCadenas = allCadenas(db);
        
        if(!mencionado.equals(usuario)){
            try (Transaction tx = db.beginTx()) {
                while(existUsers.hasNext()){
                    Node user = existUsers.next();
                    if(usuario.equals(user.getProperty("screen_name"))){
                        while(existMentionUser.hasNext()){
                            Node user2 = existMentionUser.next();
                            if(mencionado.equals(user2.getProperty("screen_name"))){
                                user.createRelationshipTo(user2, USER_RELATIONS);
                                break;
                            }
                        }
                        while(existCadenas.hasNext()){
                            Node user2 = existCadenas.next();
                            if(mencionado.equals(user2.getProperty("screen_name"))){
                                user.createRelationshipTo(user2, CADENA_RELATIONS);
                                break;
                            }
                        }
                    break;
                    }
                }
                tx.success();
            }
        }
    }
}
