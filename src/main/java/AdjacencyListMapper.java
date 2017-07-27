import org.apache.spark.api.java.function.PairFunction;

import java.util.HashMap;
import java.util.Map;

public class AdjacencyListMapper implements PairFunction<String, String,String> {
    static Map<String, Long> vertexId;
    public scala.Tuple2<String, String> call(String s){
        String parts [] = s.split("\\s+");
        for(String x:parts)
            System.out.print(x + "\t");
        String k = "";
        String [] parts2 = new String[3];
        for(int i=0; i<2; i++)
            parts2[i] = parts[i];
        for(int i=2; i<parts.length; i++)
            k = k.concat(parts[i]);
        parts2[2] = k;
        return new scala.Tuple2<String,String>(parts2[0],parts[2]);
    }
}
