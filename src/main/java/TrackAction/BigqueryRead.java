package TrackAction;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.QueryResult;

import java.util.List;
import java.util.UUID;
import java.util.Iterator;

public class BigqueryRead {
    QueryResponse getBigQuery() throws Exception{
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(
                        "SELECT * FROM `umg-tools.metadata.spotify_playlist_tracks` WHERE _PARTITIONTIME = TIMESTAMP(\"2017-08-01\");")
                        // Use standard SQL syntax for queries.
                        // See: https://cloud.google.com/bigquery/sql-reference/
                        .setUseLegacySql(false)
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
        QueryResponse response = bigquery.getQueryResults(jobId);
        return response;
    }


    public static void main(String[] args) throws Exception{
        System.out.println("Start bigquery process");
        BigqueryRead bqr = new BigqueryRead();
        QueryResponse qr = bqr.getBigQuery();
        System.out.println(qr.getResult().toString());
        QueryResult result = qr.getResult();
        Integer c = 0;

        Iterator<List<FieldValue>> iter = result.iterateAll().iterator();
        while(iter.hasNext()){
            List<FieldValue> line = iter.next();
            c++;
            for(int i=0; i<line.size();i++) {
                System.out.print(line.get(i).getValue()+"  ");
            }
            System.out.println("");
        }


        System.out.println("End of bigquery process=");
    }
}
