package TrackAction;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;

import java.util.Arrays;
import java.util.List;

public class BigqueryDataFlow {

    // --runner=DirectPipelineRunner --defaultWorkerLogLevel=ERROR --project=umg-tools --stagingLocation=gs://umg-tools/temp/dataflow --autoscalingAlgorithm=THROUGHPUT_BASED --numWorkers=10 --workerMachineType=n1-standard-4 --maxNumWorkers=10

    PCollection<KV<String,String>> bigQueryDataFlow(Pipeline pipeline, String sql) {
        PCollection<KV<String,String>> bigQueryData = pipeline
                .apply("Read", BigQueryIO.Read.fromQuery(sql).usingStandardSql())
                .apply(ParDo.of(new BQParser()));
        return bigQueryData;
    }

    public static void main(String[] args) throws Exception{
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        options.setTempLocation("gs://umg-dev/temp/dataflow");
        // Create the Pipeline object with the options we defined above.
        Pipeline pipeline = Pipeline.create(options);
        System.out.println("Start bigquery process");
        //List<String> Lines = Arrays.asList(
        //        "To be, or not to be: that is the question: ",
        //        "Whether 'tis nobler in the mind to suffer ",
        //        "The slings and arrows of outrageous fortune, ",
        //        "Or to take arms against a sea of troubles, ");
        BigqueryDataFlow bqr = new BigqueryDataFlow();
        //pipeline.apply(Create.of(Lines)).setCoder(StringUtf8Coder.of()).apply(ParDo.of(new BQPrinter()));
        //System.exit(-3);


        PCollection<KV<String,String>> bdf = bqr.bigQueryDataFlow(pipeline,"SELECT * FROM metadata.spotify_playlist_details WHERE _PARTITIONTIME = TIMESTAMP(\"2017-08-04\");");
        pipeline.run();
        System.out.println("End of bigquery process");
    }
}

