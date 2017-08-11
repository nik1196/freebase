
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;

import java.util.*;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

public class Driver {
    static JavaRDD<String> inputListRDD;

    public static JavaRDD<String> filter(Boolean byVertex){
        JavaRDD<String> filteredRDD;
        if (!byVertex){
            filteredRDD = inputListRDD.filter(new Function<String, Boolean>(){
                public Boolean call(String s){
                    if(s.split("\\s+")[2].startsWith("<"))
                        return true;
                    return false;
                }
            });
        }
        else{
            filteredRDD = inputListRDD.filter(new Function<String, Boolean>(){
                public Boolean call(String s){
                    if(s.split("\\s+")[2].startsWith("<"))
                        return false;
                    return true;
                }
            });
        }
        return filteredRDD;
    }

    public static void displayRDD(JavaPairRDD iterable, String message){
        if(!message.equals(""))
            System.out.println(message);
        Iterator iterator = iterable.toLocalIterator();
        while(iterator.hasNext())
            System.out.println(iterator.next());
        System.out.println("\n\n");
    }
    public static void displayRDD(JavaRDD iterable, String message){
        if(!message.equals(""))
            System.out.println(message);
        Iterator iterator = iterable.toLocalIterator();
        while(iterator.hasNext())
            System.out.println(iterator.next());
        System.out.println("\n\n");
    }
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        String edgeFile = "/home/nikhil/repos/freebase/Freebase100.txt";
        SparkConf conf = new SparkConf().setAppName("Freebase");
        JavaSparkContext sc = new JavaSparkContext(conf);
        inputListRDD = sc.textFile(edgeFile).cache();

        JavaRDD<String> inputEdgesRDD = filter(false);
        JavaRDD<String> backEdgesRDD = inputEdgesRDD.filter(new Function<String,Boolean>(){
            public Boolean call(String s){
                String [] parts = s.split("\\s+");
                for (int i=3;i<parts.length; i++) {
                    if (!parts[i].equals("."))
                        parts[2] = parts[2].concat(parts[i]);
                }
                if(parts[2].equals(parts[0]))
                    return false;
                return true;
            }
        }).map(new BackEdgesMapper());
        JavaPairRDD<String, String>completeEdgeList = inputEdgesRDD.union(backEdgesRDD).mapToPair(new EdgeListMapper());

        JavaRDD<scala.Tuple2<String,String>> intermediate = completeEdgeList.map(new InputVerticesGetter()).distinct();
        JavaPairRDD<String,String> vidRDD = intermediate.map(new InputSourceGetter()).distinct().mapToPair(new VidMapper2()).persist(StorageLevel.MEMORY_AND_DISK());

        JavaPairRDD<String,scala.Tuple2<String,String>> slbl_dlbl_svid = JavaPairRDD.fromJavaRDD(intermediate).join(vidRDD);

        JavaPairRDD<String, String> dlbl_svid = slbl_dlbl_svid.mapToPair(new JoinedRDDToPairRDDMapper());

        JavaPairRDD<Long, String> adjListVid = dlbl_svid.join(vidRDD).mapToPair(new JoinedRDDToPairRDDMapper2()).reduceByKey(new AdjacencyListReducer()).sortByKey();

        JavaPairRDD<String,String> adjListLabel = completeEdgeList.reduceByKey(new AdjacencyListReducer());

        JavaRDD<scala.Tuple3<String,String,String>> vertexListRDD = filter(true).map(new VertexListMapper());

        /*displayRDD(completeEdgeList, "completeEdgeList");
        displayRDD(vidRDD, "vidRDD");
        displayRDD(slbl_dlbl_svid, "slbl_dlbl_svid");
        //displayRDD(dlbl_svid, "dlbl_svid");
        displayRDD(adjListVid, "adjListVid");*/


    }
}
