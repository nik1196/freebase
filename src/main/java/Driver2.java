import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.io.File;
import java.io.FileWriter;

public class Driver2 {

    public static void main(String [] args){
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        String edgeFile = "/home/nikhil/repos/freebase/Freebase100.txt";//"nishank/sampleinput/freebase-rdf-latest.gz";
        SparkConf conf = new SparkConf().setAppName("Freebase");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);
        int total_cores = 2;// Integer.parseInt(conf.get("spark.executor.instances")) * Integer.parseInt(conf.get("spark.executor.cores"));
        JavaRDD<String> inputListRDD = sc.textFile(edgeFile).repartition(total_cores*24);
        inputListRDD = inputListRDD.filter(new Function<String, Boolean>() {
            public Boolean call(String s) {
                if (s.split("\\s+")[2].startsWith("<"))
                    return false;
                return true;
            }
        });
        JavaPairRDD<String,Long> count = inputListRDD.mapToPair(new PairFunction<String,String,Long> (){
            public scala.Tuple2<String,Long> call(String s){
                String [] parts = s.split("\\s+");
                return new scala.Tuple2<String, Long>(parts[1],1L);
            }
        });
        count = count.reduceByKey(new Function2<Long, Long, Long>() {
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong + aLong2;
            }
        });
        try {
            FileWriter fileWriter = new FileWriter("output/propertyCount");
            fileWriter.write(String.valueOf(count.count ()));
            fileWriter.close();
        }
        catch(Exception e){}

        count.saveAsTextFile("output/properties");
    }
}
