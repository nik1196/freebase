import org.apache.spark.api.java.function.PairFunction;

public class EListRetrieverMapper implements PairFunction<String,String,String> {
    public scala.Tuple2<String,String> call(String s){
        String [] parts = s.substring(1,s.length()-1).split(",");
        return new scala.Tuple2<String,String>(parts[0],parts[1]);
    }
}
