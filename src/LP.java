//package src;
//import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.graph.BasicComputation;
//import org.apache.giraph.graph.GraphState;
//import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.edge.Edge;
//import org.apache.giraph.worker.WorkerContext;
//import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.*;
import java.util.logging.Logger;

import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.List;
import java.util.*;
import java.util.logging.FileHandler;
import java.util.logging.SimpleFormatter;
import org.apache.hadoop.util.ToolRunner;
import org.apache.giraph.GiraphRunner;


//I - Vertex id
//V - Vertex data
//E - Edge data
//M - Message type


public class LP extends BasicComputation<LongWritable, VertexValue, FloatWritable, MapWritable>
{
    //Num iterations
    //public static final String NUMBER_ITERATIONS = "LP.numberiterations"; RR - do we use them?
    //public static final int DEFAULT_ITERATIONS = 20; RR - do we use them?

    public void compute(Vertex<LongWritable, VertexValue, FloatWritable> vertex, Iterable<MapWritable> messages) throws IOException
    {
        System.out.println("ENTRO");
        //HashMap<Integer,Long> mapMess = new HashMap<Integer,Long>(); RR - do we use them?
        //Iterable<Edge<LongWritable,LongWritable>> edges = vertex.getEdges(); RR - better inside?

        // Variables for messages
        //Iterator<Edge<LongWritable,LongWritable>> iterator = vertex.getEdges().iterator();
        //Edge<LongWritable,LongWritable> edges_it = iterator.next();

        //MapWritable map = new MapWritable(); 
        //IntWritable key = new IntWritable(0);
        //IntWritable key2 = new IntWritable(1); //RR-LongWriteable??

        // Superstep == 0
        if (getSuperstep() == 0)
        {
            //for all edges
            Iterator<Edge<LongWritable,FloatWritable>> iterator_e = vertex.getEdges().iterator();
            while(!iterator_e.hasNext())
            {
                Edge<LongWritable,FloatWritable> edge = iterator_e.next();
                //create message with current class and weight
                MapWritable map = new MapWritable();
                IntWritable key0 = new IntWritable(0);
                IntWritable key1 = new IntWritable(1);
                IntWritable key2 = new IntWritable(2);
                IntWritable key3 = new IntWritable(3);

                map.put(key0,new LongWritable(new Random().nextInt(2))); //Initialize the node randomly
                map.put(key1,edge.getValue()); //Edge weight 
                map.put(key2,new LongWritable(0)); //Previous label set to 0
                map.put(key3,new LongWritable(0)); //Edge weight to 0 so it doesn't affect

                sendMessage(edge.getTargetVertexId(), map);
                System.out.println("!!!!");
                System.out.println("EDGE_MAP: "+map.get(0)+" ,"+map.get(1)+" ,"+map.get(2)+" ,"+map.get(3));
            }

        }
        else {
            // No messages
            if (!messages.iterator().hasNext())
            {
                vertex.voteToHalt(); // RR -> Si no hay mensaje no se llama el metodo compute o si? igual no afecta tenerlo
            }
            //New messages
            else
            {
                //Actualizar valor de hashmap en el vertice
                Iterator iterator_m = messages.iterator();
                while (!iterator_m.hasNext())
                {
                    //obtain message values
                    MapWritable message = ((MapWritable) iterator_m.next());
                    IntWritable key0 = new IntWritable(0);
                    IntWritable key1 = new IntWritable(1);
                    IntWritable key2 = new IntWritable(2);
                    IntWritable key3 = new IntWritable(3);
                    IntWritable message_l0 = ((IntWritable) message.get(key0));
                    LongWritable message_w0 = ((LongWritable) message.get(key1));
                    IntWritable message_l1 = ((IntWritable) message.get(key2));
                    LongWritable message_w1 = ((LongWritable) message.get(key3));

                    //Create hashmap and update vertex table
                    HashMap<Integer,Long> updateClassMap = new HashMap<Integer,Long>(); 
                    updateClassMap.put(message_l0.get(),message_w0.get());
                    updateClassMap.put(message_l1.get(),message_w1.get());
                    vertex.getValue().setClasses(updateClassMap);
                }

                //Revisar si se tiene que cambiar de clase
                HashMap<Integer,Long> currClasses = vertex.getValue().getClassTable();
                //HashMap<Integer,Long> sortedCurrClasses = getMostFrequent(currClasses);

                // get classes
                LongWritable currClass = vertex.getValue().getActualCommunity();
                Long maxClass = getMostFrequent(currClasses);//sortedCurrClasses.get(sortedCurrClasses.keySet().iterator().next());

                if(currClass.get()!=maxClass){
                    //Si se cambia de clase enviar mensaje a todos los edges
                    //for all edges
                    Iterator<Edge<LongWritable,FloatWritable>> iterator_e = vertex.getEdges().iterator();
                    while(!iterator_e.hasNext())
                    {
                        Edge<LongWritable,FloatWritable> edge = iterator_e.next();
                        //create message with current class and weight
                        MapWritable map = new MapWritable();
                        IntWritable key0 = new IntWritable(0);
                        IntWritable key1 = new IntWritable(1);
                        IntWritable key2 = new IntWritable(2);
                        IntWritable key3 = new IntWritable(3);

                        map.put(key0,new LongWritable(maxClass)); //New label
                        map.put(key1,edge.getValue()); //Edge weight 
                        map.put(key2,currClass); //Previous label
                        Long valueLong = (long) edge.getValue().get()*-1;
                        map.put(key3,new LongWritable(valueLong)); //Edge weight to -w to change vote

                        sendMessage(edge.getTargetVertexId(), map);
                    }
                    //set new community
                    vertex.getValue().setActualCommunity(new LongWritable(maxClass));
                }
                // Finishes then votes to halt
                vertex.voteToHalt();
                    
                /*
                Iterator iterator_v = messages.iterator();
                while (!iterator_v.hasNext())
                {
                    MapWritable new_mess = ((MapWritable) iterator_v.next());
                    IntWritable key_1 = new IntWritable(0);
                    IntWritable mess_label = ((IntWritable)new_mess.get(key_1));

                    IntWritable key_2 = new IntWritable(1);
                    LongWritable mess_edge = ((LongWritable) new_mess.get(key_2));

                    //Look for most frequent
                    HashMap<Integer,Long> mapMess = new HashMap<Integer,Long>(); 
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

                //comm =*/
            }
        }
    }

    //Most frequent label
    private Long getMostFrequent(HashMap<Integer,Long> mapMess)
    {
       Long maxValue = -Long.MAX_VALUE;

       for (Integer key: mapMess.keySet())
       {
           if(mapMess.get(key) >= maxValue)
           {
               maxValue = mapMess.get(key);
           }
       }


    return maxValue;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new GiraphRunner(), args));
    }
}
