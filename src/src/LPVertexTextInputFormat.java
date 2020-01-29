package src;
import java.io.IOException;
import java.util.*;

import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.*;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.util.regex.Pattern;
import com.google.common.collect.Lists;
import org.apache.giraph.edge.EdgeFactory;


public class LPVertexTextInputFormat extends TextVertexInputFormat <LongWritable, VertexValue, FloatWritable>
{
    /**
     * Separator
     */
    private static final Pattern SEPARATOR = Pattern.compile(",");
    /**
     * Attributes
     */
    private int id;
    private long actComm = new Random().nextInt(2);
    private HashMap<Integer,Long> classes = new HashMap<Integer,Long> ();

    @Override
    public TextVertexReader createVertexReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
        classes.put(0,Long.MIN_VALUE);
        classes.put(1,Long.MIN_VALUE);
        classes.put(2,Long.MIN_VALUE);
        classes.put(3,Long.MIN_VALUE);
        return new VertexReader();
    }

    public class VertexReader extends
            TextVertexReaderFromEachLineProcessed<String[]> {

        @Override
        protected String[] preprocessLine(Text text) throws IOException
        {
            String[] tokens = SEPARATOR.split(text.toString());
            id = Integer.parseInt(tokens[0].replaceAll("\\s+","").replace("[","").replace("]","").replace(",",""));
            return tokens;
        }

        @Override
        protected LongWritable getId(String[] strings) throws IOException {
            return new LongWritable(id);
        }

        @Override
        protected VertexValue getValue(String[] strings) throws IOException {
            return new VertexValue(actComm, classes);
        }

        @Override
        protected Iterable<Edge<LongWritable, FloatWritable>> getEdges(String[] strings) throws IOException {

            List<Edge<LongWritable, FloatWritable>> edges = Lists.newArrayList();

            for (int n =2;n < strings.length-1;n+=2)
            {
                LongWritable vid = new LongWritable(Long.parseLong(strings[n].replaceAll("\\s+","").replace("[","").replace("]","").replace(",","")));
                FloatWritable edgeid = new FloatWritable(Long.parseLong(strings[n+1].replaceAll("\\s+","").replace("[","").replace("]","").replace(",","")));

                edges.add(EdgeFactory.create(vid, edgeid));
            }
            return edges;
        }
    }
}
