import org.apache.spark.api.java.function.PairFunction;

public class VertexListMapper implements PairFunction<String,String,String> {
    public scala.Tuple2<String, String> call(String s) {
        String[] parts = s.split("\\s+");
        String k = "";
        for (int i = 1; i < parts.length; i++)
            k += parts[i];
        scala.Tuple2<String, String> output = new scala.Tuple2<String, String>(parts[0], k);
        return output;
    }
}

