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
    private HashMap<Long,Integer> history;

    //Constructor

    public VertexValue() {

        this.actComm = 0;
        this.classes = new HashMap<Long,Float>();
        this.history = new HashMap<Long,Integer>();
    }

    public VertexValue(long currentCommunity, HashMap<Long,Float> classes, HashMap<Long,Integer> history) {

        this.actComm = currentCommunity;
        this.classes = classes;
        this.history = history;
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

        dataOutput.writeInt(this.history.size());
        for(Long h:history.keySet())
        {
            dataOutput.writeLong(h);
            dataOutput.writeInt(history.get(h));
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

        size = dataInput.readInt();
        for (int i = 0; i < size; i++)
        {
            this.history.put(dataInput.readLong(),dataInput.readInt());
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

    public void setHistory(Long key) {
        Integer newV = 1; 
        if(this.history.containsKey(key)){
            newV += this.history.get(key);
        }
        this.history.put(key,newV);
    }

    // Get methods
    public LongWritable getActualCommunity() {
        return new LongWritable(this.actComm);
    }

    public HashMap<Long,Float> getClassTable() {
        return new HashMap<Long,Float>(this.classes);
    }

    public HashMap<Long,Integer> getClassHistory() {
        return new HashMap<Long,Integer>(this.history);
    }

    //Most frequent label
    public Long getClassMostFrequent()
    {
       Long maxClass = this.actComm;
       Float maxValue = Float.MIN_VALUE;

       for (Long key: this.classes.keySet())
       {
           if(this.classes.get(key) > maxValue)
           {
                maxClass = key;
                maxValue = this.classes.get(key);
           }
       }
    return maxClass;
    }

    public Long getHistoryMinMostFrequent()
    {
        Long minMostFrequent = Long.MAX_VALUE;
        for (Long key: this.history.keySet())
        {
           if(this.history.get(key) >= 10 && key<minMostFrequent)
           {
               minMostFrequent = key;
           }
        }
        return minMostFrequent;
    }

}
