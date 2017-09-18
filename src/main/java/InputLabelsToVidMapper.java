import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.util.HashMap;


class InputLabelsToVidMapper implements PairFunction<String, String, String> {
    HashMap<String ,Long> vids;
    public InputLabelsToVidMapper(HashMap<String, Long> hashMap){
        this.vids = new HashMap<String, Long>(hashMap);
    }
    public scala.Tuple2<String, String> call(String s){
        String [] parts = s.split("\\s+");
         parts[0] = Long.toString(this.vids.get(parts[0]));
         String k = "";
         for(int i=2; i<parts.length; i++) {
             if(!parts[i].equals("."))
             k = k.concat(parts[i]);
         }
         //System.out.println("k = " + k);
         parts[2] = Long.toString(this.vids.get(k));
         return new scala.Tuple2<String, String>(parts[0], parts[2]);

    }
}


