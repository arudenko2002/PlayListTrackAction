package TrackAction;

import com.google.cloud.bigquery.*;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Create;
import org.json.JSONArray;
import org.json.JSONObject;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class SendTrackAction {
    private static String project="umg-tools";
    //private static String dataSet="metadata";
    private static String dataSet="swift_trends_alerts";
    private static String tableName="playlist_track_action";
    private static BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    private static String fields="firstname,email,artist_name,artist_uri,product_title,track_uri,name,playlist_uri,owner_id,country,followers,position,action_type,streams,estimated_streams";
    private static ReadHTTP rht=new ReadHTTP();

    static void executeQueueGeneral(String executionDate) throws Exception{
        String actualDate=getDaysAgo(executionDate,-1);
        String source=project+"."+dataSet+"."+tableName;
        String query ="SELECT "+fields+" FROM `"+source+"` WHERE _PARTITIONTIME = TIMESTAMP('"+actualDate+"') ORDER BY email,artist_name,product_title;";
        executeQueue(query,project, dataSet, tableName,actualDate);
    }

    static void executeQueue(String queue,String project, String dataSet, String tableName,String actualDate) throws Exception{
        System.out.println(queue);
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(queue)
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

        // Get the results.
        QueryResponse response = bigquery.getQueryResults(jobId);
        QueryResult result = response.getResult();
        //System.out.println("RESULT:"+result);

        //processQueryAndEmail(result);
        processQuerySaveJson(result,actualDate);
    }
/*
    private static void processQueryAndEmail(QueryResult result) throws Exception {
        String user = "swift.subscriptions@gmail.com";
        String password = "gfsniwmiqxgjoxnl";
        ArrayList<String> output = new ArrayList<String>();
        String owner_to="";
        StringBuffer oneEmail = new StringBuffer();
        String owner_fullname="";
        int counter=0;
        int tCounter=0;
        int mail_number=0;
        SSLEmail ssle = new SSLEmail();
        while (result != null) {
            for (List<FieldValue> row : result.iterateAll()) {
                HashMap<String,String> record = getRecord(row);
                String o_firstname=record.get("firstname");
                String o_email=record.get("email");
                String o_artist_name = record.get("artist_name");
                String o_product_title = record.get("product_title");
                String o_track_uri = record.get("track_uri");
                String o_playlist_name = record.get("name");
                String o_playlist_uri = record.get("playlist_uri");
                String o_owner_id = record.get("owner_id");
                String o_country = record.get("country");
                String o_followers=record.get("followers");
                String o_position = record.get("position");
                String o_action_type = record.get("action_type");
                String o_streams=record.get("streams");
                String o_estimated_strems=record.get("estimated_streams");

                String oneArtist = o_firstname+","+o_email+","+o_artist_name+","+o_product_title+","+o_track_uri+","+
                        o_playlist_name+","+ o_playlist_uri+","+o_owner_id+","+o_country+","+
                        o_followers+","+o_position+","+o_action_type+","+o_streams+","+o_estimated_strems;
                oneArtist = "<div style=\"color:red;\">"+oneArtist+"</div>";

                if((owner_to.length()==0) || (owner_to.equals(o_email)) && counter<1000) {
                    oneEmail.append((tCounter+1)+","+oneArtist+"\n");
                    owner_to=o_email;
                    owner_fullname=o_firstname;
                    counter++;
                    tCounter++;

                } else {
                    //Send Email
                    String subject="Track Actions for "+owner_fullname;
                    String body=oneEmail.toString();
                    String to = owner_to;
                    to="arudenko2002@yahoo.com";
                    mail_number++;
                    ssle.sendMail(user,password,to,subject+" #"+mail_number,body);
                    Thread.sleep(2000);
                    if ((owner_to.length()==0) || (!owner_to.equals(o_email))) {
                        tCounter = 0;
                        mail_number = 0;
                    }
                    counter=0;
                    oneEmail = new StringBuffer();
                    oneEmail.append((tCounter+1)+","+oneArtist+"\n");
                    owner_to=o_email;
                    owner_fullname=o_firstname;
                    counter++;
                    tCounter++;

                    System.out.println("COUNTER="+counter);
                    //counter=0;

                }
            }

            result = result.getNextPage();
        }
        //send email
        if(oneEmail.toString().length()>0) {
            //Send Email
            String subject="Track Actions on "+owner_fullname;
            String body=oneEmail.toString();
            String to = owner_to;
            to="arudenko2002@yahoo.com";    //Test only
            mail_number++;
            ssle.sendMail(user,password,to,subject+" #"+mail_number,body);
            System.out.println("End of COUNTER="+counter);
        }
    }
*/
    private static void processQuerySaveJson(QueryResult result, String actualDate) throws Exception {
        String f_email = "";
        String f_artist_name = "";
        String f_track_uri = "";
        //JSONObject playlist = new JSONObject();
        JSONArray playlists = new JSONArray();
        JSONObject track = new JSONObject();
        JSONArray tracks = new JSONArray();
        JSONObject artist = new JSONObject();
        JSONArray artists = new JSONArray();
        JSONObject user = new JSONObject();
        while (result != null) {
            for (List<FieldValue> row : result.iterateAll()) {
                HashMap<String,String> record = getRecord(row);
                String o_email=record.get("email");
                String o_artist_name = record.get("artist_name");
                String o_track_uri = record.get("track_uri");

                JSONObject playlist = getPlaylistJson(record);
                if(f_email.length()>0 && f_email.equals(o_email) && f_artist_name.equals(o_artist_name) && f_track_uri.equals(o_track_uri)) {
                    playlists.put(playlist);
                } else if(f_email.length()>0 && f_email.equals(o_email) && f_artist_name.equals(o_artist_name) && !f_track_uri.equals(o_track_uri)) {
                    track.put("playlists",playlists);
                    tracks.put(track);

                    //Initialize, strt new track
                    track = getTrackJson(record);
                    playlists = new JSONArray();
                    playlists.put(playlist);
                } else if(f_email.length()>0 && f_email.equals(o_email) && !f_artist_name.equals(o_artist_name)) {
                    track.put("playlists",playlists);
                    tracks.put(track);
                    artist.put("tracks",tracks);
                    artists.put(artist);

                    //Initialize new artist
                    playlists = new JSONArray();
                    playlists.put(playlist);
                    tracks = new JSONArray();
                    track = getTrackJson(record);
                    artist = getArtistJson(record);
                } else if(f_email.length()>0 && !f_email.equals(o_email)) {
                    track.put("playlists",playlists);
                    tracks.put(track);
                    artist.put("tracks",tracks);
                    artists.put(artist);
                    user.put("artists",artists);
                    saveToFile(user,actualDate);

                    //Initialize new user
                    playlists = new JSONArray();
                    playlists.put(playlist);
                    tracks = new JSONArray();
                    track = getTrackJson(record);
                    artist = getArtistJson(record);
                    artists=new JSONArray();
                    user = getUserJson(record,actualDate);
                } else if(f_email.length()==0 && o_email.length()>0) {

                    //Initial record
                    playlists = new JSONArray();
                    playlists.put(playlist);
                    track = getTrackJson(record);
                    tracks = new JSONArray();
                    artist = getArtistJson(record);
                    artists = new JSONArray();
                    user = getUserJson(record,actualDate);
                }
                f_email=record.get("email");
                f_artist_name = record.get("artist_name");
                f_track_uri = record.get("track_uri");
            }

            result = result.getNextPage();
        }
        //playlists.put(playlist);
        track.put("playlists",playlists);
        tracks.put(track);
        artist.put("tracks",tracks);
        artists.put(artist);
        user.put("artists",artists);
        saveToFile(user,actualDate);
    }

    public static void sendMailJSON(JSONObject user) throws Exception {
        String sender = "swift.subscriptions@gmail.com";
        String password = "gfsniwmiqxgjoxnl";
        SSLEmail ssle = new SSLEmail();
        JsonTemplateToHTML jth = new JsonTemplateToHTML();
        String to = user.getString("email");
        String reportdate = user.getString("reportdate");
        //to="alexey.rudenko@umusic.com";
        String subject = "TEST: Track Activity Report";
        //System.out.println(user);
        String body = jth.processJson(user,reportdate);
        jth.saveHTML("resources/email_result_test.html",body);
        ssle.sendMail(sender,password,to,subject,body);
    }

    public static void saveToFile(JSONObject user, String actualDate) throws Exception {
        sendMailJSON(user);
        if(false) {
            ArrayList<String> output = new ArrayList<String>();
            output.add(user.toString());
            System.out.println(user);
            String[] args = {""};
            String output_bucket = "umg-swift-trends-alerts-triggers";
            PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
            options.setTempLocation("gs://umg-tools/temp/dataflow");
            options.setRunner(BlockingDataflowPipelineRunner.class);
            //options.setRunner(DataflowPipelineRunner.class);
            System.out.println("Runner=" + options.getRunner());
            // Create the Pipeline object with the options we defined above.
            Pipeline pipeline = Pipeline.create(options);
            pipeline.apply(Create.of(output)).setCoder(StringUtf8Coder.of())
                    .apply(TextIO.Write.to("gs://" + output_bucket + "/alert_triggers/email_" + actualDate + "_" + user.getString("email") + "/" + "mail.json"));
            pipeline.run();
        }
    }

    private static JSONObject getPlaylistJson(HashMap<String,String> record) {
        JSONObject playlist = new JSONObject();
        playlist.put("name",record.get("name"));
        playlist.put("playlist_uri",record.get("playlist_uri"));
        playlist.put("owner_id",record.get("owner_id"));
        playlist.put("country",record.get("country"));
        playlist.put("followers",record.get("followers"));
        playlist.put("position",record.get("position"));
        playlist.put("action_type",record.get("action_type"));
        playlist.put("streams",record.get("streams"));
        playlist.put("estimated_streams",record.get("estimated_streams"));
        return playlist;
    }

    private static JSONObject getTrackJson(HashMap<String,String> record) {
        JSONObject track = new JSONObject();
        track.put("product_title",record.get("product_title"));
        track.put("track_uri",record.get("track_uri"));
        track.put("track_image",record.get("track_image"));
        return track;
    }

    private static JSONObject getArtistJson(HashMap<String,String> record) {
        JSONObject artist = new JSONObject();
        artist.put("artist_name",record.get("artist_name"));
        artist.put("artist_image",record.get("artist_image"));
        artist.put("artist_uri",record.get("artist_uri"));
        return artist;
    }

    private static JSONObject getUserJson(HashMap<String,String> record, String actualDate) {
        JSONObject user = new JSONObject();
        user.put("firstname",record.get("firstname"));
        user.put("email",record.get("email"));
        user.put("reportdate",actualDate);
        return user;
    }

    private static HashMap getRecord(List<FieldValue> row) throws Exception{
        HashMap<String,String> result = new HashMap<String,String>();
        String[] ss = fields.split(",");
        for(int i=0; i<ss.length;i++) {
            //System.out.println(ss[i]+"   "+row.get(i).getValue());
            result.put(ss[i], row.get(i).getValue().toString());
        }
        if(result.get("track_uri")!=null && result.get("track_uri").length()>0) {
            String track_uri = result.get("track_uri").split(":")[2];
            //ReadHTTP rht=new ReadHTTP();
            System.out.println("_TTTOKEN="+rht.auth+"     "+rht);
            rht.getImages(track_uri);
            result.put("track_image",rht.track_image);
            result.put("artist_image",rht.artist_image);
        }
        return result;
    }

    public String getToday() throws ParseException {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date date = new Date();
        return dateFormat.format(date);
    }

    static public String getDaysAgo(String day, int daysago) throws ParseException {
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

    public static void main(String[] args) throws Exception{
        System.out.println("Start process");
        SendTrackAction  sta = new SendTrackAction();
        String executionDate=sta.getToday();
        for (int i=0; i< args.length;i++) {
            if(args[i].equals("-project")) sta.project=args[i+1];
            if(args[i].equals("-dataSet")) sta.dataSet=args[i+1];
            if(args[i].equals("-tableName")) sta.tableName=args[i+1];
            if(args[i].equals("-executionDate")) executionDate=args[i+1];
        }

        executionDate="2017-08-05"; // Test date
        sta.executeQueueGeneral(executionDate);
        System.out.println("End of process");
    }
}
