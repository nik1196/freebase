import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;

public class InputVerticesGetter implements PairFlatMapFunction<scala.Tuple2<String,String>, String,String> {
    public Iterator<Tuple2<String, String>> call(scala.Tuple2<String,String> s) {
        String [] parts = s._2().split("\\s+");
        ArrayList<scala.Tuple2<String,String>> list = new ArrayList<scala.Tuple2<String, String>>();
        list.add(new scala.Tuple2<String,String>(s._1(),parts[1]));
        list.add(new scala.Tuple2<String,String>(parts[1],s._1()));
        return list.iterator();
    }
}
