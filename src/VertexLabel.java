import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class VertexLabel implements Writable
{
    private long actComm;
    private long lastComm;

    //Constructor
    public VertexLabel(long currentCommunity, long lastCommunity) {

        this.actComm = currentCommunity;
        this.lastComm = lastCommunity;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeLong(this.actComm);
        dataOutput.writeLong(this.lastComm);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        this.actComm = dataInput.readLong();
        this.lastComm = dataInput.readLong();
    }

    // Change methods
    public void setLastCommunity(LongWritable lastComm) {
        this.lastComm = lastComm.get();
    }

    public void setActualCommunity(LongWritable actComm) {
        this.actComm  = actComm.get();
    }

    // Get methods
    public LongWritable getActualCommunity() {
        return new LongWritable(this.actComm);
    }

    public LongWritable getLastCommunity() {
        return new LongWritable(this.lastComm);
    }
}
