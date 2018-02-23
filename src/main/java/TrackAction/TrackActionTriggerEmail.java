package TrackAction;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * An example that joins together 3 tables
 * By Alexey Rudenko
 */
public class TrackActionTriggerEmail {

    public static void moveMongoDBtoBQ(String project) throws Exception{
        MemoryDataFlow bqr = new MemoryDataFlow();
        bqr.moveMongoDBtoBQ(project);
    }

    public static void createTableTriggerEmail(String executionDate,String project,String dataSet,String tableName) throws Exception{
        System.out.println("Create the table with partitions, fill the partition, create the trigger file");
        CreateTablePartition  ctp = new CreateTablePartition();
        ctp.procedure(executionDate,project,dataSet,tableName);
        System.out.println("End of the process");
    }

    public String getToday() throws ParseException {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date date = new Date();
        return dateFormat.format(date);
    }

    public static void main(String[] args) throws Exception{
        System.out.println("Start process");
        TrackActionTriggerEmail tate = new TrackActionTriggerEmail();
        String project="umg-dev";
        //String dataSet="metadata";
        String dataSet="swift_trends_alerts";
        String tableName="playlist_track_action";
        String executionDate=tate.getToday();
        for (int i=0; i< args.length;i++) {
            if(args[i].equals("-project")) project=args[i+1];
            if(args[i].equals("-dataSet")) dataSet=args[i+1];
            if(args[i].equals("-tableName")) tableName=args[i+1];
            if(args[i].equals("-executionDate")) executionDate=args[i+1];
        }
        tate.moveMongoDBtoBQ(project);
        //MongoDBDataFlow mdbbq = new MongoDBDataFlow();
        //mdbbq.noveMongoDBtoBigQuery(args);
        executionDate="2017-08-05";
        tate.createTableTriggerEmail(executionDate,project,dataSet,tableName);
        System.out.println("End of process");
    }
}

