package utils_project;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;

public class Writer {

    public void writeString(ArrayList<String> file) throws IOException, URISyntaxException {

        //1. Get the instance of Configuration
        Configuration configuration = new Configuration();

        String fileName = "test_file";


        //2. URI of the file to be write
        URI uri = new URI("hdfs://localhost:54310/simone/sabd");

        //3. Get the instance of the HDFS
        FileSystem hdfs = FileSystem.get(uri, configuration);

        //==== Write file
//Create a path
        Path hdfswritepath = new Path(uri + "/" + fileName);
//Init output stream
        FSDataOutputStream outputStream=hdfs.create(hdfswritepath);
//Cassical output stream usage
        outputStream.writeBytes(file.get(0));
        outputStream.close();
    }


    public void writeRdd(JavaPairRDD rdd) throws IOException, URISyntaxException {

        //1. Get the instance of Configuration
        Configuration configuration = new Configuration();

        String fileName = "test_rdd";


        //2. URI of the file to be write
        URI uri = new URI("hdfs://localhost:54310/simone/sabd");

        //3. Get the instance of the HDFS
        FileSystem hdfs = FileSystem.get(uri, configuration);

        //==== Write file
        //Create a path
        Path hdfswritepath = new Path(uri + "/" + fileName);
        //Init output stream
        FSDataOutputStream outputStream=hdfs.create(hdfswritepath);
        //Cassical output stream usage
        outputStream.writeBytes(rdd.collect().toString());
        outputStream.close();
    }
}
