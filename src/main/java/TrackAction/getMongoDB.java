package TrackAction;

import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;

import com.mongodb.ServerAddress;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.mongodb.client.MongoCursor;
import org.json.JSONArray;
import org.json.JSONObject;

public class getMongoDB {
//    host: process.env.MONGO_HOST || '35.185.196.180',
//    port: process.env.MONGO_PORT || '27017',
//    database: process.env.MONGO_DB || 'sst-metadata-dev',
//    readPreference: process.env.MONGO_READ_PREFERENCE || 'secondary',
//    username: process.env.MONGO_USERNAME || 'consumption-dev',
//    password: process.env.MONGO_PASSWORD || 'aeKei0EeyiGhoaD3',
//    authSource: process.env.MONGO_AUTH_SOURCE || 'sst-metadata-dev',
    static String host = "35.197.2.144";
    static int port = 27017;
    static String databaseA = "sst-dizzee-dev";
    static String user = "dizzee-dev";
    static String password = "phioR6teAithei6I";
    static String collectionA="users";

    public void readMongoDB() {
        MongoCredential credential = MongoCredential.createCredential(user, databaseA, password.toCharArray());
        MongoClient mongoClient = new MongoClient(new ServerAddress(host, 27017), Arrays.asList(credential));
        // Creating a Mongo client
        MongoDatabase database = mongoClient.getDatabase(databaseA);
        MongoCollection<Document> collection = database.getCollection(collectionA);
        System.out.println("Connected to the database successfully");
        System.out.println("Collection sampleCollection selected successfully");
        System.out.println("Total="+collection.count());
        // Getting the iterable object
        FindIterable<Document> iterDoc = collection.find();
        int i = 1;
        // Getting the iterator
        MongoCursor<Document> it = iterDoc.iterator();
        while (it.hasNext()) {
            JSONObject obj = new JSONObject(it.next().toJson());
            if(obj.keySet().contains("firstName") && obj.keySet().contains("lastName"))
                System.out.println(obj.get("firstName")+"  "+obj.get("lastName")+"   "+i);
            i++;
        }
    }

    public ArrayList<String> readMongoDBUri() {
        ArrayList<String> result = new ArrayList<String>();

        String uri = "mongodb://"+user+":"+password+"@"+host+":"+port+"/"+databaseA;
        System.out.println(uri);
        MongoClientURI mongoUri  = new MongoClientURI(uri);
        MongoClient mongoClient = new MongoClient(mongoUri);
        MongoDatabase database = mongoClient.getDatabase(mongoUri.getDatabase());
        MongoCollection<Document> collection = database.getCollection(collectionA);

        System.out.println("Connected to the database successfully");
        System.out.println("Collection sample, Collection selected successfully");
        System.out.println("Total="+collection.count());
        // Getting the iterable object
        FindIterable<Document> iterDoc = collection.find();
        int i = 1;
        // Getting the iterator
        MongoCursor<Document> it = iterDoc.iterator();
        while (it.hasNext()) {
            JSONObject obj = new JSONObject(it.next().toJson());
            if(obj.keySet().contains("firstName")
                    && obj.keySet().contains("lastName")
                    && obj.keySet().contains("email")
                    && obj.keySet().contains("interests")
                    && obj.keySet().contains("active") && Boolean.parseBoolean(obj.get("active").toString())
                    && obj.keySet().contains("subscriptions") && Boolean.parseBoolean(obj.getJSONObject("subscriptions").get("playlistSubscription").toString())
                    ) {
                String firstName = obj.get("firstName").toString();
                String lastName = obj.get("lastName").toString();
                String email = obj.get("email").toString();
                JSONObject o = (JSONObject)obj.get("interests");
                JSONArray conopusids = (JSONArray)o.get("artists");
                for(int j=0; j<conopusids.length();j++) {
                    System.out.println(firstName+","+lastName + "," + email+","+conopusids.get(j));
                    result.add(firstName+","+lastName + "," + email+","+conopusids.get(j));
                }

                            }
            i++;
        }
        return result;
    }

    public static void main( String args[] ) {
        getMongoDB gmdb = new getMongoDB();
        //gmdb.readMongoDB();
        gmdb.readMongoDBUri();

    }
}
