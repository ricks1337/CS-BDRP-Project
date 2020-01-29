//package src;
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
    private static final Pattern SEPARATOR = Pattern.compile("[\t ]");
    /**
     * Attributes
     */
    private int id;
    private long actComm = Long.MAX_VALUE;
    private HashMap<Integer,Long> classes = new HashMap<Integer,Long> ();

    @Override
    public TextVertexReader createVertexReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
        classes.put(0,Long.MAX_VALUE);
        classes.put(1,Long.MAX_VALUE);
        classes.put(2,Long.MAX_VALUE);
        classes.put(3,Long.MAX_VALUE);
        return new VertexReader();
    }

    public class VertexReader extends
            TextVertexReaderFromEachLineProcessed<String[]> {

        @Override
        protected String[] preprocessLine(Text text) throws IOException
        {
            String[] tokens = SEPARATOR.split(text.toString());
            id = Integer.parseInt(tokens[0].replace("[","").replace("]","").replace(",",""));
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

            int n = 2;
            while (n < strings.length-1)
            {
                edges.add(EdgeFactory.create(new LongWritable(Long.parseLong(strings[n])),
                        new FloatWritable(Long.parseLong(strings[n+1]))));
                n = n+2;
            }
            return edges;
        }
    }
}
