package TrackAction;

import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import org.json.JSONObject;

import java.util.Iterator;
import java.util.List;

public class BQParser extends DoFn<TableRow,KV<String,String>> {
    int counter=0;
    @Override
    public void processElement(ProcessContext c) throws Exception {
        TableRow line = c.element();
        System.out.println(line);
        JSONObject obj = new JSONObject(line);
        String key = obj.getString("name");
        String value = obj.getString("playlist_id")+","+obj.getString("owner_id")+","+obj.getString("type");
        System.out.println("key="+key+" value="+value+"  "+counter);
        c.output(KV.of(key, value));
        counter++;
    }
}
