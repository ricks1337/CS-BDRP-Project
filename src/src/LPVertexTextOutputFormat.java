package src;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LPVertexTextOutputFormat  extends TextVertexOutputFormat <LongWritable, VertexValue, FloatWritable>
{
    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException
    {
        return new LPTextVertexLineWriter();
    }

    private class LPTextVertexLineWriter extends TextVertexWriterToEachLine
    {
        @Override
        protected Text convertVertexToLine(Vertex<LongWritable, VertexValue, FloatWritable> vertex) throws IOException
        {
            StringBuilder sb = new StringBuilder(vertex.getId().toString());
            sb.append(" ");
            sb.append(vertex.getValue().getActualCommunity().get());
            return new Text(sb.toString());
        }
    }
}