package src;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;


public class VertexValue implements Writable
{
    private long actComm;
    private HashMap<Long,Float> classes;

    //Constructor

    public VertexValue() {

        this.actComm = 0;
        this.classes = new HashMap<Long,Float>();
    }

    public VertexValue(long currentCommunity, HashMap<Long,Float> classes) {

        this.actComm = currentCommunity;
        this.classes = classes;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeLong(this.actComm);

        dataOutput.writeInt(this.classes.size());

        for(Long c:classes.keySet())
        {
            dataOutput.writeLong(c);
            dataOutput.writeFloat(classes.get(c));
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        this.actComm = dataInput.readLong();
        int size = dataInput.readInt();

        for (int i = 0; i < size; i++)
        {
            this.classes.put(dataInput.readLong(),dataInput.readFloat());
        }
    }

    // Change methods

    public void setActualCommunity(LongWritable actComm) {
        this.actComm  = actComm.get();
    }

    public void setClasses(HashMap<Long,Float> c) {
        for (Long key:c.keySet())
        {
            Float newV = c.get(key);
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
    }

    public HashMap<Long,Float> getClassTable() {
        return new HashMap<Long,Float>(this.classes);
    }

}
