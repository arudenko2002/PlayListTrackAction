package TrackAction;

import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import org.json.JSONObject;

import java.util.Iterator;
import java.util.List;

public class BQPrinter extends DoFn<String,KV<String,String>> {
    int counter=0;
    @Override
    public void processElement(ProcessContext c) throws Exception {
        String line = c.element();
        //System.out.println(line);
        //JSONObject obj = new JSONObject(line);
        //String key = obj.getString("name");
        //String value = obj.getString("playlist_id")+","+obj.getString("owner_id")+","+obj.getString("type");
        String key=line;
        String value=line;
        System.out.println("key2="+key+" value2="+value+"  "+counter);
        c.output(KV.of(key, value));
        counter++;
    }
}