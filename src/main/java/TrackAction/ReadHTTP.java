package TrackAction;
import java.io.IOException;
import okhttp3.Headers;
import okhttp3.OkHttpClient;
import okhttp3.OkHttpClient.Builder;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Authenticator;
import okhttp3.Route;
import org.json.JSONArray;
import org.json.JSONObject;

public class ReadHTTP {
    static String auth = "Bearer BQA-8Tc-ktLSjuJc13M-4MqYt3FJE2SFCKH-QVA4N48IYkg08eEnUWgFzAdEnPN8lPsbubOfsC_FbZu6Vo7qoA";

    String track_image = "";
    String artist_image = "";
    String url="https://api.spotify.com/v1/tracks/";

    private String getBody(String href) throws Exception {
        String body = "";
        int counter=0;
        while(true) {
            counter++;
            if(counter>10) {
                System.out.println("The call was done "+counter+" times without success. exiting...");
                return "";
            }
            OkHttpClient client = new OkHttpClient.Builder()
                    .authenticator(new Authenticator() {
                        @Override
                        public Request authenticate(Route route, Response response) throws IOException {
                            return response.request().newBuilder()
                                    .header("Authorization", auth)
                                    .build();
                        }
                    })
                    .followRedirects(true)
                    .followSslRedirects(true)
                    .build();
            //System.out.println(client);

            Request request = new Request.Builder()
                    .url(href)
                    .get()
                    .addHeader("content-type", "application/json; charset=UTF-8")
                    .addHeader("authorization", auth)
                    .build();
            //System.out.println(request);
            try {
                System.out.println("API request to spotify");
                Response response = client.newCall(request).execute();
                body = response.body().string();
                Thread.sleep(2 * 1000);
                break;
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (java.net.ProtocolException e) {
                synchronized(this) {
                    RefreshToken rt = new RefreshToken();
                    auth = "Bearer " + rt.refreshToken();
                    System.out.println("_TOKEN=" + auth);
                }
            }
        }
        return body;
    }

    private String getTrackImageBody(String body) {
        JSONObject b = new JSONObject(body);
        JSONArray images = b.getJSONObject("album").getJSONArray("images");
        JSONObject image =images.getJSONObject(2);
        return image.getString("url");
    }

    private String getArtistHref(String body) {
        JSONObject b = new JSONObject(body);
        JSONArray artists = b.getJSONObject("album").getJSONArray("artists");
        JSONObject href =artists.getJSONObject(0);
        return href.getString("href");
    }

    private String getArtistImageBody(String body) {
        JSONObject b = new JSONObject(body);
        JSONArray images = b.getJSONArray("images");
        JSONObject href =images.getJSONObject(2);
        return href.getString("url");
    }

    public void getImages(String uri) throws Exception{
        // Read track data
        String track_json_body = getBody(url+uri);
        // Get track image
        track_image = getTrackImageBody(track_json_body);
        // Get artist href from track data
        String artist_href = getArtistHref(track_json_body);
        // Read arstist data
        String artist_json_body = getBody(artist_href);
        // Get artist image url
        artist_image = getArtistImageBody(artist_json_body);
    }

    public static void main(String[] args) throws Exception {
        ReadHTTP rhp = new ReadHTTP();
        //rhp.getImages("568BqBOqxp0xyv93dmjv3Q");
        rhp.getBody("https://api.spotify.com/v1/tracks/568BqBOqxp0xyv93dmjv3Q");
        System.out.println("track="+rhp.track_image);
        System.out.println("artist="+rhp.artist_image);
    }
}
