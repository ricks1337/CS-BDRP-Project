package src;
import java.io.IOException;
import java.util.*;
import java.lang.*;

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
    private long id;
    private long actComm = new Random(System.nanoTime()).nextLong();
    private HashMap<Long,Float> classes = new HashMap<Long,Float> ();

    @Override
    public TextVertexReader createVertexReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
        return new VertexReader();
    }

    public class VertexReader extends
            TextVertexReaderFromEachLineProcessed<String[]> {

        @Override
        protected String[] preprocessLine(Text text) throws IOException
        {
            String[] tokens = SEPARATOR.split(text.toString());
            id = Long.parseLong(tokens[0].replaceAll("\\s+","").replace("[","").replace("]","").replace(",",""));
            actComm = Long.parseLong(tokens[1].replaceAll("\\s+","").replace("[","").replace("]","").replace(",",""));
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
                FloatWritable edgeW = new FloatWritable(Long.parseLong(strings[n+1].replaceAll("\\s+","").replace("[","").replace("]","").replace(",","")));

                edges.add(EdgeFactory.create(vid, edgeW));
            }
            return edges;
        }
    }
}
