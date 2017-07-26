import org.apache.spark.api.java.function.PairFunction;

import java.util.HashMap;

public class EdgeListMapper implements PairFunction<String,String,String> {
    HashMap<String ,Long> vids;
    public EdgeListMapper(HashMap<String, Long> hashMap){
        this.vids = new HashMap<String, Long>(hashMap);
    }
    public scala.Tuple2<String, String> call(String s){
        String [] parts = s.split("\\s+");
        parts[0] = Long.toString(vids.get(parts[0]));
        String k = "";
        for(int i=2; i<parts.length; i++) {
            if(!parts[i].equals("."))
                k = k.concat(parts[i]);
        }
        //System.out.println("k=\n" + k);
        parts[2] = Long.toString(vids.get(k));
        return new scala.Tuple2<String, String>(parts[0] + parts[2], parts[1]);
    }
}
