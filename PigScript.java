package logsparser;

import java.io.IOException;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;

public class LogsParser {

    public static void main(String[] args) {

        try {

            PigServer pigServer = new PigServer(ExecType.MAPREDUCE); // running on HDFS mode
            pigScript(pigServer);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // method to execute Pig scripts
    public static void pigScript(PigServer pigServer) throws IOException {
        pigServer.registerJar("/user/lib/pig/piggybank.jar;");
        pigServer.registerQuery("DEFINE CommonLogLoader org.apache.pig.piggybank.storage.apachelog.CommonLogLoader();");
        pigServer.registerQuery("logs_raw = LOAD '/user/cloudera/access_log' USING CommonLogLoader"
                + "AS (user_ip, domain_log, authentication, time, request_method,uri, protocol, response_code, size);");
        pigServer.store(logs_raw, "logs_output");
    }
}
