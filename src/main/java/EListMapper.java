import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.util.HashMap;

public class EListMapper implements PairFunction<String, String, String> {

    public Tuple2<String, String> call(String s){
        String [] parts = s.split("\\s+");
        for(int i=3;i<parts.length;i++) {
            if (!parts[i].equals("."))
                parts[2] = parts[2].concat(parts[i]);
        }
        return new Tuple2<String, String>(parts[0],parts[1] + " " + parts[2]);
    }
}
