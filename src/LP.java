
import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.*;


//I - Vertex id
//V - Vertex data
//E - Edge data
//M - Message type
public class LP extends BasicComputation<LongWritable, VertexLabel , LongWritable, LongWritable>
{
    //Num iterations
    public static final String NUMBER_ITERATIONS = "LP.numberiterations";
    public static final int DEFAULT_ITERATIONS = 20;

    public void compute(Vertex<LongWritable, VertexLabel, LongWritable> vertex, Iterable<LongWritable> messages) throws IOException
    {
        List<Long> mges = new ArrayList<>();
        long comm;
        // No messages
        if(!messages.iterator().hasNext())
        {
            comm = vertex.getValue().getActualCommunity().get();
        }
        //New messages
        else
        {
            for (LongWritable m : messages) {
                mges.add(m.get());
            }

            //comm =
        }
    }

    //Most frequent label
    private long getMostFrequent(Vertex<LongWritable, VertexLabel, LongWritable> vertex, List<Long> messages)
    {
        List<Long> sameFreqs = new ArrayList<>();
        Set<Long> distinct = new HashSet<Long>(messages);
        int maxFreq = 1;
        Long maxNeigh = messages.get(0);

        for (Long l: distinct)
        {
           int freq = Collections.frequency(messages,l);

           //Get max freq
           if(freq > maxFreq)
            {
               maxFreq = freq;
               maxNeigh = l;
            }

        }

        //Check for ties
        return 0;
    }
}
