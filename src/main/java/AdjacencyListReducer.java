import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

public class AdjacencyListReducer implements Function2<String, String, String> {
    public String call(String a, String b){return a + " " + b;}
}
