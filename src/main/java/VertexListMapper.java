import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple3;

public class VertexListMapper implements PairFunction<String, String, String> {
    public scala.Tuple2<String, String> call(String s) {
        String[] parts = s.split("\\s+");
        for (int i = 3; i < parts.length; i++)
            if(!parts[i].equals("."))
                parts[2] = parts[2].concat(parts[i]);
        return new scala.Tuple2<String, String>(parts[0], "pname!"+ parts[1] + "$pvalue!"+ parts[2]+"$ ");
    }
}

