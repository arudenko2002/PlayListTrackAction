package TrackAction;

import java.util.Properties;
import javax.mail.*;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

public class SSLEmail {
    private void sendJustMe(MimeMessage msg) throws Exception{
        msg.setRecipients(Message.RecipientType.TO,"alexey.rudenko@umusic.com,arudenko2002@yahoo.com");
    }
    private void sendGroup(MimeMessage msg) throws Exception{
        String recipients = "alexey.rudenko@umusic.com";
        recipients += ",Tom.Hovis@umusic.com";
        recipients += ",Justin.Roth@umusic.com";
        recipients += ",Srikanth.Komatireddy@umusic.com";
        recipients += ",Gevorg@umusic.com";
        recipients += ",KFK@umusic.com";
        msg.setRecipients(Message.RecipientType.TO, recipients);
    }
    public int sendMail(String email,String app_password, String to, String subject, String body) throws Exception{
        final String  d_email = email,
                m_to = to,
                m_subject = subject,
                m_text = body,
                d_uname = email,
                d_password=app_password,
                d_host = "smtp.gmail.com",
                d_port  = "465";

        Properties props = new Properties();
        props.put("mail.smtp.user", d_email);
        props.put("mail.smtp.host", d_host);
        props.put("mail.smtp.port", d_port);
        props.put("mail.smtp.starttls.enable","true");
        props.put("mail.smtp.debug", "true");
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.socketFactory.port", d_port);
        props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
        props.put("mail.smtp.socketFactory.fallback", "false");

        Session session = Session.getInstance(props,
                new javax.mail.Authenticator() {
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(d_uname, d_password);
                    }
                });
        session.setDebug(true);
        MimeMessage msg = new MimeMessage(session);
        try {
            msg.setSubject(m_subject);
            msg.setFrom(new InternetAddress(d_email));
            msg.addRecipient(Message.RecipientType.TO, new InternetAddress(m_to));
            sendJustMe(msg);
            msg.setSubject(m_subject);
            msg.setContent(m_text,"text/html");

            Transport transport = session.getTransport("smtps");//transport.connect();
            transport.connect(d_host, Integer.valueOf(d_port), d_uname, d_password);
            transport.sendMessage(msg, msg.getAllRecipients());
            transport.close();

        } catch (AddressException e) {
            e.printStackTrace();
            return -1;
        } catch (MessagingException e) {
            e.printStackTrace();
            return -1;
        }
        System.out.println("Email sent: "+m_to);
        return 0;
    }

    /**
     Outgoing Mail (SMTP) Server
     requires TLS or SSL: smtp.gmail.com (use authentication)
     Use Authentication: Yes
     Port for SSL: 465
     */
    public static void main(String[] args) throws Exception {
        SSLEmail ss = new SSLEmail();
        //ss.sendMail("arudenko2002@gmail.com","fxhcwbmwerkypvwd","alexey.rudenko@umusic.com","Probka arudenko2002@gmail.com","Probka is.");
        //ss.sendMail("swift.subscriptions@gmail.com","gfsniwmiqxgjoxnl","arudenko2002@yahoo.com","Probka swift@umusic.com","Probka is.");
        ss.sendMail("alexey.rudenko2002@gmail.com","ugfepuhzgzfbybgj","arudenko2002@yahoo.com","Probka alexey.rudenko2002@gmail.com","Probka is.");
    }
}
