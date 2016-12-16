
package neo4j;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.json.simple.parser.ParseException;
/**
 *
 * @author patricio
 */
public class Neo4J {
    
    public enum Usuarios implements Label {
	USUARIO,CADENA;
    }
    
    public enum Relationships implements RelationshipType{
	USER_RELATIONS,CADENA_RELATIONS;
    }
    
    public static void main(String [] args) throws IOException, ParseException {
        GraphDatabaseFactory dbFactory = new GraphDatabaseFactory();
        GraphDatabaseService db = dbFactory.newEmbeddedDatabase(new File("/home/patricio/Descargas/neo4j-community-3.0.6/data/databases/graph.db"));
                
        Tweet tweet = new Tweet();
        RequestDataBase rdb = new RequestDataBase();
        
        File [] archivos = tweet.extractTweets();
        tweet.readTweet(archivos, db);
        tweet.createUserMentions(archivos, db);
//        List<Integer> result = rdb.searchNodeByName(db, "Just_Beeeeeee",Usuarios.USUARIO, RelationshipType.withName("USER_RELATIONS"), RelationshipType.withName("CADENA_RELATIONS"));
//        System.out.println("Seguidos " + result.get(0));
//        System.out.println("Seguidores" + result.get(1));
//        System.out.println("Grado con los usuarios " + result.get(2));
//        System.out.println("Grado con las cadenas " + result.get(3));
//        System.out.println("Done successfully");
    }
    
}
