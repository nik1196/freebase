import org.apache.spark.api.java.function.PairFunction;

import java.util.HashMap;
import java.util.Map;

public class AdjacencyListMapper implements PairFunction<String, String,String> {
    long vid = 0;
    static Map<String, Long> vertexId;
    public scala.Tuple2<String, String> call(String s){
        String parts [] = s.split("\\s+");
        String k = "";
        String [] parts2 = new String[2];
        for(int i=0; i<2; i++)
            parts2[i] = parts[i];
        for(int i=2; i<parts.length; i++)
            k += parts[i];
        parts2[2] = k;
        scala.Tuple2<String,String> output = new scala.Tuple2<String,String>(parts2[0],parts[2]);
        return output;
    }
}
