package TrackAction;


import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.*;
//import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.values.PCollection;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeoutException;

public class CreateTablePartition {
    String project="umg-tools";
    //ßString dataSet="metadata";
    String dataSet = "swift_trends_alerts";
    String tableName="playlist_track_action";
    String query="SELECT * FROM ";
    private static BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

    public static boolean checkTableExists(String datasetId, String tableName) {
        Table table = bigquery.getTable(datasetId, tableName);
        if (table == null) {
            return false;
        }
        return true;
    }

    static void createTable(String project, String dataSet,String tableName) {
        //CreateTablePartition ctp = new CreateTablePartition();
        TableId tableId = null;
        //String project="umg-tools";
        //String tableName="test_table";
        //String dataSet="metadata";
        if (checkTableExists(dataSet,tableName)) {
            System.out.println(tableName+" already exists, exiting...");
            return;
        }
        if (StringUtils.isNotEmpty(project)) {
            tableId = TableId.of(project, dataSet, tableName);
        } else {
            tableId = TableId.of(dataSet, tableName);
        }

        List<Field> fields = new ArrayList<>();
        fields.add(Field.of("transaction_date", Field.Type.timestamp()));
        fields.add(Field.of("product_id", Field.Type.string()));
        fields.add(Field.of("sales_country_code", Field.Type.string()));
        Schema schema = Schema.of(fields);

        StandardTableDefinition.Builder builder = StandardTableDefinition.newBuilder()
                .setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY));
                //.setSchema(schema);

        TableDefinition tableDefinition = builder.build();
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        Table table = bigquery.create(tableInfo);
        System.out.println("Table created");
    }

    static void executeQueue(String queue,String project, String dataSet, String tableName,String actualDate) throws Exception{
        System.out.println(queue);
        System.out.println("ACTUAL DATE="+actualDate+"  project="+project+"  ds="+dataSet+"  tableName="+tableName+" actusalDate="+actualDate);
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(queue)

                        //.setUseLegacySql(false)
                        .setDestinationTable(TableId.of(project,dataSet, tableName+"$"+actualDate))
                        .setUseLegacySql(false)
                        .setFlattenResults(true)
                        .setAllowLargeResults(true)
                        .setPriority(QueryJobConfiguration.Priority.BATCH)
                        //.setUseQueryCache(false)
                        .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
                        .setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE)


                        .build();

        // Create a job ID so that we can safely retry.
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        // Wait for the query to complete.
        queryJob = queryJob.waitFor();

        // Check for errors
        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            // You can also look at queryJob.getStatus().getExecutionErrors() for all
            // errors, not just the latest one.
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }

        // Get the results.

        QueryResponse response = bigquery.getQueryResults(jobId);

        QueryResult result = response.getResult();
        //System.out.println("RESULT:"+result);

        // Print all pages of the results.
        ArrayList<String> output = new ArrayList<String>();
        while (result != null) {
            for (List<FieldValue> row : result.iterateAll()) {
                StringBuffer sb = new StringBuffer();
                for(int i=0; i<row.size();i++) {


                    if(row.get(i).getValue()!=null) {
                        String v = row.get(i).getValue().toString();
                        System.out.print(v + ",");
                        sb.append(row.get(i).getValue()+",");
                    } else {
                        System.out.print("null,");
                        sb.append("null,");
                    }
                }
                System.out.println("");
                output.add(sb.toString());
            }

            result = result.getNextPage();
        }
        //saveToGCS(output,project,actualDate);
    }

    public String getDaysAgo(String day, int daysago) throws ParseException {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date date = new Date();
        if (day!=null) {
            date = dateFormat.parse(day);
        }
        GregorianCalendar cal = new GregorianCalendar();
        cal.setTime(date);
        cal.add(Calendar.DATE, daysago);
        Date newDate = cal.getTime();
        String newDateStr = dateFormat.format(newDate);
        return newDateStr;
    }

    public static void saveToGCS(List<String> output,String project,String actualDate){
        String[] args = {""};
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        options.setTempLocation("gs://umg-tools/temp/dataflow");
        options.setRunner(BlockingDataflowPipelineRunner.class);
        //options.setRunner(DataflowPipelineRunner.class);
        System.out.println("Runner="+options.getRunner());
        // Create the Pipeline object with the options we defined above.
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply(Create.of(output)).setCoder(StringUtf8Coder.of())
                .apply(TextIO.Write.to("gs://"+project+"/files_triggers/"+actualDate+".csv"));
        pipeline.run();
    }

    public void procedure(String executionDate,String project, String dataSet, String tableName) throws Exception{
        System.out.println("EXECUTION DATE="+executionDate);
        File file = new File("resources/insert.sql");
        Scanner sc = new Scanner(file);
        sc.useDelimiter("\\Z");
        String sql = sc.next().replace("{ExecutionDate}",executionDate);
        String actualDate = getDaysAgo(executionDate,-1).replaceAll("-","");
        System.out.println("SSSS="+actualDate);
        System.out.println(sql);
        if(!checkTableExists(dataSet,tableName)) {
            createTable(project,dataSet,tableName);
        }
        executeQueue(sql,project,dataSet, tableName, actualDate);
    }

    public static void main(String[] args) throws Exception{
        System.out.println("Create Table with Partition");
        CreateTablePartition  ctp = new CreateTablePartition();
        String project="umg-tools";
        String dataSet="swift_trends_alerts";
        String tableName="playlist_track_action";
        ctp.procedure("2017-08-05",project,dataSet,tableName);
    }
}
