import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;


public class VertexLabel implements Writable
{
    private long actComm;
    private HashMap<Integer,Long> classes;

    //Constructor
    public VertexLabel(long currentCommunity, HashMap<Integer,Long> classes) {

        this.actComm = currentCommunity;
        this.classes = classes;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeLong(this.actComm);

        dataOutput.writeInt(this.classes.size());

        for(Integer c:classes.keySet())
        {
            dataOutput.writeInt(c);
            dataOutput.writeLong(classes.get(c));
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        this.actComm = dataInput.readLong();
        int size = dataInput.readInt();

        for (int i = 0; i < size; i++)
        {
            this.classes.put(dataInput.readInt(),dataInput.readLong());
        }
    }

    // Change methods

    public void setActualCommunity(LongWritable actComm) {
        this.actComm  = actComm.get();
    }

    public void setClasses(HashMap<Integer,Long> c) {
        for (Integer key:c.keySet())
        {
            Long newV = c.get(key);
            if(this.classes.containsKey(key))
            {
                newV+=classes.get(key);
            }
            classes.put(key,newV);
        }
    }

    // Get methods
    public LongWritable getActualCommunity() {
        return new LongWritable(this.actComm);
    }5

}
