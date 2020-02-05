package src;
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
import java.lang.*;
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
        System.out.println("Nanoi:" + System.nanoTime());
        System.out.println("Milii:" + System.currentTimeMillis());
        // Superstep == 0
        if (getSuperstep() == 0)
        {
            //for all edges
            Iterator<Edge<LongWritable,FloatWritable>> iterator_e = vertex.getEdges().iterator();
            while(iterator_e.hasNext())
            {
                Edge<LongWritable,FloatWritable> edge = iterator_e.next();
                //create message with current class and weight
                MapWritable map = new MapWritable();
                IntWritable key0 = new IntWritable(0);
                IntWritable key1 = new IntWritable(1);
                IntWritable key2 = new IntWritable(2);
                IntWritable key3 = new IntWritable(3);

                map.put(key0,vertex.getValue().getActualCommunity()); //Initialize the node
                map.put(key1,edge.getValue()); //Edge weight 
                map.put(key2,new LongWritable(0)); //Previous label set to 0
                map.put(key3,new FloatWritable(0)); //Edge weight to 0 so it doesn't affect

                sendMessage(edge.getTargetVertexId(), map);                
            }
            //System.out.println("Sup0:");
            //System.out.println("ID: " + vertex.getId().toString() + " Community: " + vertex.getValue().getActualCommunity().get());
        }
        else {
            //System.out.println("Sup"+getSuperstep()+":");
            //System.out.println("ID: " + vertex.getId().toString() + " Community: " + vertex.getValue().getActualCommunity().get());
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
                while (iterator_m.hasNext())
                {
                    //obtain message values
                    MapWritable message = ((MapWritable) iterator_m.next());
                    IntWritable key0 = new IntWritable(0);
                    IntWritable key1 = new IntWritable(1);
                    IntWritable key2 = new IntWritable(2);
                    IntWritable key3 = new IntWritable(3);
                    LongWritable message_l0 = ((LongWritable) message.get(key0));
                    FloatWritable message_w0 = ((FloatWritable) message.get(key1));
                    LongWritable message_l1 = ((LongWritable) message.get(key2));
                    FloatWritable message_w1 = ((FloatWritable) message.get(key3));

                    //Create hashmap and update vertex table
                    HashMap<Long,Float> updateClassMap = new HashMap<Long,Float>(); 
                    updateClassMap.put(message_l0.get(),message_w0.get());
                    updateClassMap.put(message_l1.get(),message_w1.get());
                    vertex.getValue().setClasses(updateClassMap);
                }

                //Revisar si se tiene que cambiar de clase
                boolean changed=false;
                
                // get classes
                LongWritable currClass = vertex.getValue().getActualCommunity();
                Long maxClass = vertex.getValue().getClassMostFrequent();
                //registers class in history
                vertex.getValue().setHistory(maxClass);

                if (currClass.get()!=maxClass) {
                    changed = true;
                    //make sure the class is not rotating
                    Long minHistory = vertex.getValue().getHistoryMinMostFrequent();
                    if(minHistory!=Long.MAX_VALUE){
                        if(minHistory==currClass.get()){
                            changed = false;
                        }
                        maxClass = minHistory;
                    }
                }

                if(changed){
                    //Si se cambia de clase enviar mensaje a todos los edges
                    //for all edges
                    Iterator<Edge<LongWritable,FloatWritable>> iterator_e = vertex.getEdges().iterator();
                    while(iterator_e.hasNext())
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
                        float valueLong = (float) edge.getValue().get()*-1;
                        map.put(key3,new FloatWritable(valueLong)); //Edge weight to -w to change vote

                        sendMessage(edge.getTargetVertexId(), map);
                    }
                    //set new community
                    vertex.getValue().setActualCommunity(new LongWritable(maxClass));
                }

                // Finishes then votes to halt
                vertex.voteToHalt();
            }
        }
        System.out.println("Nanof:" + System.nanoTime());
        System.out.println("Milif:" + System.currentTimeMillis());
    }

    

}
