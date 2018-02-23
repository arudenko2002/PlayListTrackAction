package TrackAction;

/*

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.json.JSONObject;

import java.util.Arrays;


import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
//import com.google.common.annotations.VisibleForTesting;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableRow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.transforms.ParDo;

import java.util.ArrayList;
import java.util.List;


public class MongoDBDataFlow {
    static String user  = "swift-dev-admin";
    static String password = "eef9XeifIemiech8";

    static String host = "35.197.2.144";
    static int port = 27017;
    static String databaseA = "sst-dizzee-dev";
    static String collectionA = "users";

    static String uri = "mongodb://"+user+":"+password+"@"+host+":"+port+"/?authMechanism=SCRAM-SHA-1&authSource=admin";

    public void mongoDBDataFlow(Pipeline pipeline) {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("firstname").setType("STRING"));
        fields.add(new TableFieldSchema().setName("lastname").setType("STRING"));
        fields.add(new TableFieldSchema().setName("email").setType("STRING"));
        fields.add(new TableFieldSchema().setName("conopus_id").setType("INTEGER"));
        TableSchema schema = new TableSchema().setFields(fields);

        System.out.println(uri);
        MongoDbIO.Read readMongoDB = MongoDbIO.read()
                .withUri(uri)
                .withDatabase(databaseA)
                .withCollection(collectionA);

        BigQueryIO.Write<TableRow> writeBigQuery = BigQueryIO.writeTableRows()
                .to("umg-tools:swift_trends_alerts.temp_output_table2")
                .withSchema(schema)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED);

        pipeline.apply(readMongoDB)
                .apply(ParDo.of(new MDBParser()))
                .apply(writeBigQuery);
    }

    public void noveMongoDBtoBigQuery(String[] args) {
        System.out.println("MongoDBDataFlow started");
        org.apache.beam.sdk.options.PipelineOptions options = org.apache.beam.sdk.options.PipelineOptionsFactory.fromArgs(args).create();
        options.setTempLocation("gs://umg-dev/temp/dataflow");
        // Create the Pipeline object with the options we defined above.
        org.apache.beam.sdk.Pipeline pipeline = org.apache.beam.sdk.Pipeline.create(options);
        mongoDBDataFlow(pipeline);
        pipeline.run().waitUntilFinish();
        System.out.println("End of MongoDB process");
    }

    public static void main(String args[]) {
        MongoDBDataFlow mdb = new MongoDBDataFlow();
        mdb.noveMongoDBtoBigQuery(args);
    }
}
*/