package gcfv2pubsub;

import com.google.cloud.functions.CloudEventsFunction;
import com.google.events.cloud.pubsub.v1.MessagePublishedData;
import com.google.events.cloud.pubsub.v1.Message;
import com.google.gson.Gson;
import io.cloudevents.CloudEvent;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Base64;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;


import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

import javax.sql.DataSource;

public class PubSubFunction implements CloudEventsFunction {
  private static final Logger logger = Logger.getLogger(PubSubFunction.class.getName());

  private static final String DB_IP_ADDRESS = System.getenv("DB_IP_ADDRESS");
  private static final String DB_NAME = System.getenv("DB_NAME");

  private static final String DB_CONNECTION_URL = "jdbc:mysql://" + DB_IP_ADDRESS + ":" + "3306" + "/" + DB_NAME;
  private static final String DB_PASSWORD = System.getenv("DB_PASSWORD");
  private static final String DB_USER = System.getenv("DB_USER");

  public void accept(CloudEvent event) {
    // Get cloud event data as JSON string
    String cloudEventData = new String(event.getData().toBytes());
    // Decode JSON event data to the Pub/Sub MessagePublishedData type
    Gson gson = new Gson();
    MessagePublishedData data = gson.fromJson(cloudEventData, MessagePublishedData.class);
    // Get the message from the data
    Message message = data.getMessage();
    // Get the base64-encoded data from the message & decode it
    String encodedData = message.getData();
    String decodedData = new String(Base64.getDecoder().decode(encodedData));

    String[] parts = decodedData.split(":");

    String uuid = parts[0];
    String email = parts[1];

    // Log the message
    logger.info("uuid: " + uuid + "email: " + email);

    String verificationLink = "https://csye6225-cloud-project.me./v1/user/verify?token=" + uuid + ":" + email;

    try {
      sendVerificationEmail(email, verificationLink);

      updateEmailSentTime(email);
    } catch (UnirestException e) {
      logger.severe("Error sending email: " + e.getMessage());
    }
  }

  private void updateEmailSentTime(String email) {
    try (Connection conn = createConnectionPool().getConnection()) {
        String stmt = "UPDATE cloudDatabase.users SET email_expiry_time = DATE_ADD(NOW(), INTERVAL 2 MINUTE) WHERE username =?";
        try (PreparedStatement voteStmt = conn.prepareStatement(stmt)) {
          voteStmt.setString(1, email);
          voteStmt.execute();
          logger.info("Updated for user: " + email);
        }
      } catch (SQLException ex) {
        logger.log(Level.SEVERE, "Error while attempting to update", ex);
      }
  }

  public static DataSource createConnectionPool() {
    logger.info("createConnectionPool");
    // The configuration object specifies behaviors for the connection pool.
    HikariConfig config = new HikariConfig();
    // Configure which instance and what database user to connect with.
    config.setJdbcUrl(String.format(DB_CONNECTION_URL));
    config.setUsername(DB_USER);
    config.setPassword(DB_PASSWORD);

    return new HikariDataSource(config);
  }


  private void sendVerificationEmail(String email, String verificationLink) throws UnirestException {
    HttpResponse<JsonNode> request = Unirest.post("https://api.mailgun.net/v3/" + "csye6225-cloud-project.me" + "/messages")
            .basicAuth("api", "8bb52f00ee1ee6b889bdf71f7dae647a-f68a26c9-3e94c1f9")
            .queryString("from", "Excited User <user@csye6225-cloud-project.me>")
            .queryString("to", email)
            .queryString("subject", "Email Verification")
            .queryString("text", "Click the following link to verify your email: " + verificationLink)
            .asJson();
    logger.info("Request body: " + request.getBody());
  }
}
