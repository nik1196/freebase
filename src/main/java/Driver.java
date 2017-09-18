
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;

import java.util.*;
import com.google.common.base.Optional;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.broadcast.Broadcast;

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
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //int total_cores = Integer.parseInt(conf.get("spark.executor.instances")) * Integer.parseInt(conf.get("spark.executor.cores"));
        inputListRDD = sc.textFile(edgeFile).repartition(10);//total_cores*2);

        JavaRDD<String> inputEdgesRDD = filter(false);
     /*   JavaRDD<String> backEdgesRDD = inputEdgesRDD.filter(new Function<String,Boolean>(){
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
        }).map(new BackEdgesMapper());*/
        JavaPairRDD<String, String>completeEdgeList = inputEdgesRDD.union(inputEdgesRDD.filter(new Function<String,Boolean>(){
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
        }).map(new BackEdgesMapper())).mapToPair(new EdgeListMapper());
        Broadcast<JavaPairRDD<String,String>> intermediateBroadcast = sc.broadcast(completeEdgeList.mapToPair(new InputVerticesGetter()).distinct());
        JavaPairRDD<String,Long> vidRDD = intermediateBroadcast.value().map(new InputSourceGetter()).distinct().zipWithIndex().mapToPair(new VidMapper2());
        /*JavaPairRDD<Long, String> adjListVid = */intermediateBroadcast.value().join(vidRDD).mapToPair(new JoinedRDDToPairRDDMapper()).join(vidRDD).mapToPair(new JoinedRDDToPairRDDMapper2()).reduceByKey(new AdjacencyListReducer()).sortByKey();//.saveAsTextFile("FreebaseMetis");

        //JavaPairRDD<String,String> adjListLabel = completeEdgeList.reduceByKey(new AdjacencyListReducer());

        JavaPairRDD<String,String> srcPropRDD = filter(true).mapToPair(new VertexListMapper()).reduceByKey(new VertexListReducer()).mapToPair(new VertexPropertyNumberAssigner());

        JavaPairRDD<String, scala.Tuple2<Long,Optional<String>>> srcLabel_srcId_srcProp= vidRDD.leftOuterJoin(srcPropRDD);

        JavaPairRDD<scala.Tuple2<String,String>,String > srcLabelSnkLabel_Eprops = completeEdgeList.mapToPair(new SinkGetter()).join(vidRDD).mapToPair(new SinkLabelReformatterMapper()).mapToPair(new EpropAsValue_Mapper()).reduceByKey(new Eprop_AsValue_Reducer());
        JavaPairRDD<String, String> srcLabel_SnkLabelEprops = srcLabelSnkLabel_Eprops.mapToPair(new EdgePropertyNumberAssignerMapper()).mapToPair(new Snk_Eprop_AsValue_Mapper()).reduceByKey(new Snk_Eprop_AsValue_Reducer());
        JavaPairRDD<Long, scala.Tuple2<String, String>> finalJsonFormat = srcLabel_srcId_srcProp.join(srcLabel_SnkLabelEprops).mapToPair(new Label_Id_Swap_Mapper());
        finalJsonFormat.saveAsTextFile("jsonFormat");

        //displayRDD(completeEdgeList, "completeEdgeList");
        //displayRDD(vidRDD, "vidRDD");
        //displayRDD(slbl_dlbl_svid, "slbl_dlbl_svid");
        //displayRDD(dlbl_svid, "dlbl_svid");
        //displayRDD(adjListVid, "adjListVid");
        displayRDD(finalJsonFormat, "finalJsonFormat");


    }
}
