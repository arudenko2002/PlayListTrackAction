package TrackAction;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.DoFn;

public class GetTableRow extends DoFn<String,TableRow> {
        int counter=0;
        @Override
        public void processElement(ProcessContext c) throws Exception {
        String line = c.element();

        String[] values = line.split(",");

        TableRow row = new TableRow()
            .set("firstname", values[0])
            .set("lastname", values[1])
            .set("email", values[2])
            .set("conopus_id", values[3]);
        c.output(row);
        //System.out.println("ROW="+row);
        counter++;
        }
}