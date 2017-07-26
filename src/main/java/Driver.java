import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import com.google.common.collect.Lists;
import java.util.*;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
public class Driver {
    static JavaRDD<String> inputListRDD;

    public static JavaRDD<String> filter(Boolean byVertex){
        JavaRDD<String> filteredRDD;
        if (!byVertex){
            filteredRDD = inputListRDD.filter(new Function<String, Boolean>(){
                public Boolean call(String s){
                    if(s.split("\\t")[2].startsWith("<"))
                        return true;
                    return false;
                }
            });
        }
        else{
            filteredRDD = inputListRDD.filter(new Function<String, Boolean>(){
                public Boolean call(String s){
                    if(s.split("\\t")[2].startsWith("<"))
                        return false;
                    return true;
                }
            });
        }
        return filteredRDD;
    }


    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        String edgeFile = "/nishank/sampleinput/Freebase100.txt";
        SparkConf conf = new SparkConf().setAppName("Freebase");
        //conf.setMaster("");
        JavaSparkContext sc = new JavaSparkContext(conf);
        inputListRDD = sc.textFile(edgeFile).cache();

        JavaRDD<String> inputEdgesRDD = filter(false);

        JavaPairRDD<String,String> adjListRDD = inputEdgesRDD.mapToPair(new AdjacencyListMapper());

        Iterator<scala.Tuple2<String,String>> adjListIterator = adjListRDD.toLocalIterator();
        List<scala.Tuple2<String,String>> adjListList = Lists.newArrayList(adjListIterator);
        HashMap<String,Long> vids = new HashMap<String, Long>(); //vertex-vid mapping

        for(int i=0; i<adjListList.size();i++) {
            scala.Tuple2<String,String> currentVertexList = adjListList.get(i);
            if(!vids.containsKey(currentVertexList._1)){
                vids.put(currentVertexList._1, (long)vids.size()+1);
            }
            if(!vids.containsKey(currentVertexList._2)){
                vids.put(currentVertexList._2, (long)vids.size()+1);
            }
        }
        //System.out.println(vids.toString());
        JavaPairRDD<String,String> edgeListRDD = inputEdgesRDD.mapToPair(new EdgeListMapper(vids)).reduceByKey(new EdgeListReducer());

        JavaPairRDD<String,String> vertexListRDD = filter(true).mapToPair(new VertexListMapper()).reduceByKey(new VertexListReducer());



        
        JavaRDD<String> backEdgesRDD = inputEdgesRDD.map(new BackEdgesMapper());
        
        
        
        inputListRDD = inputListRDD.union(backEdgesRDD);

        adjListRDD = inputEdgesRDD.mapToPair(new InputLabelsToVidMapper(vids)).reduceByKey(new InputLabelsToVidReducer());
        Iterator<scala.Tuple2<String,String>> iterator_adjListRDD = adjListRDD.toLocalIterator();
        while(iterator_adjListRDD.hasNext()) {
            System.out.println(iterator_adjListRDD.next());
        }
    }
}
