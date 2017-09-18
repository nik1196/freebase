import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BackEdgesMapper implements PairFlatMapFunction<Tuple2<String,String>,String,String> {
    public Iterator<Tuple2<String,String>> call(scala.Tuple2<String,String> s){
        String [] parts = s._2().split("\\s+");
        List<scala.Tuple2<String,String>> list = new ArrayList<Tuple2<String,String>>();
        list.add(s);
        list.add(new scala.Tuple2<String,String>(parts[1], "<> " + s._1()));
        return list.iterator();
    }
}
