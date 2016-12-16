
package neo4j;

import java.util.ArrayList;
import java.util.List;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

/**
 *
 * @author patricio
 */

public class RequestDataBase {
    
    //Get a especific node by name, how this is unique doesn't problem
    public List<Integer> searchNodeByName(GraphDatabaseService db, String nameNode, Label label, RelationshipType ur, RelationshipType cr) {
        List<Integer> response = new ArrayList<Integer>();
        try (Transaction tx = db.beginTx()) {
             
            Node result = db.findNode(label, "screen_name", nameNode);
            
            int gradeUser = result.getDegree(ur);
            int gradeCadena = result.getDegree(cr);
            response.add(Integer.parseInt(result.getProperty("follows_amount").toString()));
            response.add(Integer.parseInt(result.getProperty("followers_amount").toString()));
            response.add(gradeUser);
            response.add(gradeCadena);
            tx.success();
            return response;
        }
    }
    
}
