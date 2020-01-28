
import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.*;
import java.util.stream.Collectors;


//I - Vertex id
//V - Vertex data
//E - Edge data
//M - Message type


public class LP extends BasicComputation<LongWritable, VertexLabel , LongWritable, MapWritable>
{
    //Num iterations
    public static final String NUMBER_ITERATIONS = "LP.numberiterations";
    public static final int DEFAULT_ITERATIONS = 20;

    public void compute(Vertex<LongWritable, VertexLabel, LongWritable> vertex, Iterable<MapWritable> messages) throws IOException
    {
        HashMap<Integer,Long> mapMess = new HashMap<Integer,Long>();
        Iterable<Edge<LongWritable,LongWritable>> edges = vertex.getEdges();

        // Variables for messages
        Iterator<Edge<LongWritable,LongWritable>> iterator = vertex.getEdges().iterator();


        MapWritable map = new MapWritable();
        IntWritable key = new IntWritable(0);
        IntWritable key2 = new IntWritable(1);

        // Superstep == 0
        if (getSuperstep() == 0)
        {
            while(!iterator.hasNext())
            {
                Edge<LongWritable,LongWritable> edges_it = iterator.next();
                map.put(key,vertex.getValue().getActualCommunity()); //Actual label
                map.put(key2,edges_it.getValue()); //Edge weight

                sendMessage(edges_it.getTargetVertexId(), map);
            }

        }
        else {

            // No messages
            if (!messages.iterator().hasNext())
            {
                //?¿
            }
            //New messages
            else
                {

                Iterator iterator_v = messages.iterator();
                while (!iterator_v.hasNext())
                {
                    MapWritable new_mess = ((MapWritable) iterator_v.next());
                    IntWritable key_1 = new IntWritable(0);
                    IntWritable mess_label = ((IntWritable)new_mess.get(key_1));

                    IntWritable key_2 = new IntWritable(1);
                    LongWritable mess_edge = ((LongWritable) new_mess.get(key_2));

                    //Look for most frequent
                    mapMess.put(mess_label.get(),mess_edge.get());
                    HashMap<Integer,Long> sorted_mess = getMostFrequent(mapMess);

                    Iterator<Integer> iterator_sort = sorted_mess.keySet().iterator();
                    Long valmax = sorted_mess.get(iterator_sort.next());

                    //Update vertex
                    vertex.getValue().setActualCommunity(new LongWritable(valmax));
                    vertex.getValue().setClasses(sorted_mess);

                }

                //Send messages
                    while(!iterator.hasNext())
                    {
                        Edge<LongWritable,LongWritable> edges_it = iterator.next();

                        map.put(key,vertex.getValue().getActualCommunity()); //Actual label
                        map.put(key2,edges_it.getValue()); //Edge weight

                        sendMessage(edges_it.getTargetVertexId(), map);
                    }

                //comm =
            }
        }
    }

    //Most frequent label
    private HashMap<Integer, Long> getMostFrequent(HashMap<Integer,Long> mapMess)
    {
        HashMap<Integer, Long> sorted = mapMess
                .entrySet()
                .stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new));

    return mapMess;
    }
}
