import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.giraph.conf.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;

public class GiraphAppRunner<main> implements Tool
{
    private Configuration conf;
    private String inputPath;

    public String getInputPath() {
        return inputPath;
    }

    public void setInputPath(String inputPath) {
        this.inputPath = inputPath;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    private String outputPath;

    @Override
    public int run(String[] args) throws Exception {

        setInputPath("data/SPN-Giraph.txt");
        setOutputPath("output/GiraphOutputPath");

        GiraphConfiguration gconf = new GiraphConfiguration(getConf());
        gconf.setComputationClass(LP.class);
        gconf.setVertexInputFormatClass(LPVertexTextInputFormat.class);
        //conf.setVertexOutputFormatClass(AdjacencyListTextVertexOutputFormat.class);
        GiraphFileInputFormat.addVertexInputPath(gconf,new Path (getInputPath()));

        gconf.setWorkerConfiguration(0,1,100);
        gconf.setLocalTestMode(true);
        gconf.setMaxNumberOfSupersteps(20);

        gconf.SPLIT_MASTER_WORKER.set(gconf,false);
        gconf.USE_OUT_OF_CORE_GRAPH.set(gconf,true);

        GiraphJob job = new GiraphJob(gconf,getClass().getName());

        FileOutputFormat.setOutputPath(job.getInternalJob(),new Path(getOutputPath()));

        job.run(true);
        return 1;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;

    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    public static void main (String[] args) throws Exception
    {
        ToolRunner.run(new GiraphAppRunner(),args);
    }
}
