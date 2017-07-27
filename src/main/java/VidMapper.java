import org.apache.spark.api.java.function.Function2;
import com.google.common.collect.Lists;
import scala.Tuple4;

import java.util.*;

public class VidMapper implements Function2<Integer, Iterator<scala.Tuple3<String,String,String>>, Iterator<scala.Tuple2<String,String>>> {
    HashMap<String,Long> vids;
    long count = 0;
    public VidMapper(){
        this.vids = new HashMap<String, Long>();
    }
    public Iterator<scala.Tuple2<String,String>> call(Integer v1, Iterator<scala.Tuple3<String,String,String>> v2) throws Exception {
        List<scala.Tuple2<String,String>> retList = Lists.newArrayList();
        List<scala.Tuple3<String,String,String>> list = Lists.newArrayList(v2);
        for(int i=0; i<list.size(); i++){
            scala.Tuple3<String,String,String> s = list.get(i);
            //System.out.println(s)
            long cur_vid;
            if(!vids.containsKey(s._1())){
                cur_vid = (((long) v1)<<40) + ++count;
                vids.put(s._1(), cur_vid);
                //System.out.println(parts[0]);
                //System.out.println(vids.toString());
            }
            else
                cur_vid = (((long) v1)<<40) + count;
            retList.add(new scala.Tuple2<String,String>(String.valueOf(cur_vid), list.get(i)._1()));
        }
        return retList.iterator();
    }
}
