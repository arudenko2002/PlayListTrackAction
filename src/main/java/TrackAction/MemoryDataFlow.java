package TrackAction;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.api.services.bigquery.model.TableRow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import TrackAction.getMongoDB;

public class MemoryDataFlow {


    PCollection<KV<String,String>> memoryDataFlow(Pipeline pipeline, List<String> lines) {
        PCollection<KV<String, String>> pcollection = pipeline
                .apply(Create.of(lines)).setCoder(StringUtf8Coder.of())
                .apply(ParDo.of(new BQPrinter()));
        return pcollection;
    }

    static private PCollection<TableRow> memoryDataFlowTableRow(Pipeline pipeline, List<String> lines) {
        PCollection<TableRow> pcollection = pipeline
                .apply(Create.of(lines)).setCoder(StringUtf8Coder.of())
                .apply(ParDo.of(new GetTableRow()));
        return pcollection;
    }

    static private void storeToBigQuery(PCollection<TableRow> mongodatacollection) {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("firstname").setType("STRING"));
        fields.add(new TableFieldSchema().setName("lastname").setType("STRING"));
        fields.add(new TableFieldSchema().setName("email").setType("STRING"));
        fields.add(new TableFieldSchema().setName("conopus_id").setType("INTEGER"));
        TableSchema schema = new TableSchema().setFields(fields);
        mongodatacollection.apply(BigQueryIO.Write
                .named("Write")
                .to("umg-tools:swift_trends_alerts.temp_output_table")
                .withSchema(schema)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
    }

    public static void moveMongoDBtoBQ(String project) throws Exception{
        System.out.println("Start mongodb->pcollection process");
        String[] args={"--project="+project};
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        options.setTempLocation("gs://umg-dev/temp/dataflow");
        //options.setRunner(DirectPipelineRunner.class);
        options.setRunner(BlockingDataflowPipelineRunner.class);
        System.out.println("Runner="+options.getRunner());
        // Create the Pipeline object with the options we defined above.
        Pipeline pipeline = Pipeline.create(options);
        System.out.println("Start mongodb->pcollection process");
        getMongoDB mongodata = new getMongoDB();
        ArrayList<String> data = mongodata.readMongoDBUri();
        //MemoryDataFlow bqr = new MemoryDataFlow();
        //bqr.memoryDataFlow(pipeline,data);
        PCollection<TableRow> mongodatacollection = memoryDataFlowTableRow(pipeline,data);
        storeToBigQuery(mongodatacollection);
        pipeline.run();
        System.out.println("End of mongodb->pcollection process");
    }

    public static void main(String[] args) throws Exception{
        MemoryDataFlow bqr = new MemoryDataFlow();
        bqr.moveMongoDBtoBQ("umg-dev");
    }
}

