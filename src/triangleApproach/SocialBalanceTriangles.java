package triangleApproach;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;
import java.io.File;
import java.io.FileWriter;
import com.opencsv.CSVWriter;
import java.io.IOException;
import java.util.*;
import org.neo4j.driver.v1.Record;

public class SocialBalanceTriangles implements AutoCloseable
{

    private final Driver driver;

    private static String filename = "SPN";

    public SocialBalanceTriangles ( String uri, String user, String password )
    {
        driver = GraphDatabase.driver( uri, AuthTokens.basic( user, password ) );
    }

    public void loadGraph(String filename)
    {
        try ( Session session = driver.session() ) {

                    session.run( "LOAD CSV WITH HEADERS FROM "+filename+" AS row " +
                                    "MERGE (v1:Vertex {Id: row.Source}) " +
                                    "MERGE (v2:Vertex {Id: row.Target}) " +
                                    "CREATE (v1)-[r:weight{weight:row.Weight}]->(v2)");
        }
    }

    public List<Record> triangles()
    {
        try ( Session session = driver.session() ) {

            StatementResult sr = session.run( "CALL algo.triangle.stream('Vertex','weight') "+
                                                 "YIELD nodeA,nodeB,nodeC " +
                                                "WITH algo.asNode(nodeA).Id AS nodeA, algo.asNode(nodeB).Id AS nodeB, algo.asNode(nodeC).Id AS nodeC " +
                                                "MATCH (n1:Vertex)-[w1:weight]-(n2:Vertex)-[w2:weight]-(n3:Vertex)-[w3:weight]-(n1) " +
                                                "WHERE n1.Id = nodeA and n2.Id = nodeB and n3.Id = nodeC "+
                                                "WITH distinct n1.Id as n1, w1.weight as w1, n2.Id as n2, w2.weight as w2, n3.Id as n3,w3.weight as w3, toInt(w1.weight)*toInt(w2.weight)*toInt(w3.weight) as mult "+
                                                "RETURN n1, "+
                                                "(case when mult > 0 then toFloat(w1)*2 else toFloat(w1) end) as w1, "+
                                                "n2, "+
                                                "(case when mult > 0 then toFloat(w2)*2 else toFloat(w2) end) as w2, "+
                                                "n3, "+
                                                "(case when mult > 0 then toFloat(w3)*2 else toFloat(w3) end) as w3; ");

            return sr.list();
        }
    }

    public static void writeCSV(String filePath, List<Record> list) {
        // first create file object for file placed at location
        // specified by filepath
        File file = new File(filePath);
        try
        {
            // create FileWriter object with file as parameter
            FileWriter outputfile = new FileWriter(file);
            CSVWriter writer = new CSVWriter(outputfile);

            String[] header = { "Source", "Target", "Weight" };
            writer.writeNext(header);

            List<String[]> data = new ArrayList<String[]>();

            for (int i =0; i < list.size();i++)
            {
                String [][] tuples = new String [3][3];
                tuples[0][0]=list.get(i).get("n1").toString();
                tuples[0][1]=list.get(i).get("n2").toString();
                tuples[0][2]=list.get(i).get("w1").toString();
                tuples[1][0]=list.get(i).get("n2").toString();
                tuples[1][1]=list.get(i).get("n3").toString();
                tuples[1][2]=list.get(i).get("w2").toString();
                tuples[2][0]=list.get(i).get("n3").toString();
                tuples[2][1]=list.get(i).get("n1").toString();
                tuples[2][2]=list.get(i).get("w3").toString();

                for (int j = 0; j < tuples.length;j++)
                {
                    data.add(tuples[j]);
                }

            }
            writer.writeAll(data);
            writer.close();
        }

        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

        @Override
    public void close() throws Exception {
        driver.close();



    }

    public static void main (String[] args) throws Exception
    {

        SocialBalanceTriangles sbt= new SocialBalanceTriangles("bolt://localhost:7687","neo4j", "graph");
        sbt.loadGraph("\"file:/"+filename+".csv\"");
        sbt.writeCSV("resources/"+filename+"-triangles.csv",sbt.triangles());
        sbt.close();
    }
}
