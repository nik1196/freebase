import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class AdjListWithVidMapper implements PairFunction<Tuple2<String, Tuple2<String,String>>, String, Tuple2<String,String>> {
    JavaRDD<scala.Tuple2<String,String>> vidRDD;
    public AdjListWithVidMapper(JavaRDD<Tuple2<String,String>> vidRDD){
        this.vidRDD = vidRDD;
    }

    public Tuple2<String, Tuple2<String,String>> call(Tuple2<String, Tuple2<String,String>> s){
        return new Tuple2<String, Tuple2<String, String>>(null,null);
    }
}
