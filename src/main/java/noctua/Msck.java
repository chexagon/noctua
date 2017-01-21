package noctua;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.amazonaws.athena.jdbc.AthenaDriver;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;

/**
 * Runs MSCK REPAIR TABLE
 *
 * Created by trieut on 1/20/17.
 */
public class Msck {
    static final String DATABASE_PARAM = "database";
    static final String AWS_ACCOUNTID_PARAM = "aws.accountid";
    static final String AWS_REGION_PARAM = "aws.region";
    static final String AWS_REGION = System.getProperty(AWS_REGION_PARAM, "us-east-1");
    static final String ATHENA_URL = "jdbc:awsathena://athena." + AWS_REGION + ".amazonaws.com:443";
    static final String DATABASE = System.getProperty(DATABASE_PARAM, "default");
    static final String AWS_ACCOUNTID = System.getProperty(AWS_ACCOUNTID_PARAM);
    static final String S3_STAGING_DIR = "s3://aws-athena-query-results-"
            + AWS_ACCOUNTID + "-" + AWS_REGION + "/noctua";

    public static void main(String[] args) {
        List<String> availableTables = new ArrayList<>();

        Connection listTablesConnection = null;
        Statement listTablesStatement = null;
        try {
            listTablesConnection = getConnection();
            listTablesConnection.setReadOnly(true);

            String listTablesQuery = "SHOW TABLES IN " + DATABASE;
            listTablesStatement = listTablesConnection.createStatement();
            ResultSet rs = listTablesStatement.executeQuery(listTablesQuery);

            while (rs.next()) {
                //Retrieve table column.
                String tableName = rs.getString("tab_name");
                availableTables.add(tableName);
            }

            rs.close();
            listTablesConnection.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            try {
                if (listTablesStatement != null)
                    listTablesStatement.close();
            } catch (Exception ex) {

            }
            try {
                if (listTablesConnection != null)
                    listTablesConnection.close();
            } catch (Exception ex) {

                ex.printStackTrace();
            }
        }

        for (String table : args) {
            if (! availableTables.contains(table)) {
                continue;
            }

            Connection msckConnection = null;
            Statement msckStatement = null;
            String msck = "MSCK REPAIR TABLE " + DATABASE + "." + table;
            try {
                msckConnection = getConnection();
                msckStatement = msckConnection.createStatement();
                ResultSet rs = msckStatement.executeQuery(msck);
                rs.close();
                msckConnection.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                try {
                    if (msckStatement != null)
                        msckStatement.close();
                } catch (Exception ex) {

                }
                try {
                    if (msckConnection != null)
                        msckConnection.close();
                } catch (Exception ex) {

                    ex.printStackTrace();
                }
            }
        }

    }

    private static Connection getConnection()
            throws ClassNotFoundException, SQLException
    {
        Class.forName("com.amazonaws.athena.jdbc.AthenaDriver");
        Properties info = new Properties();
        info.put("s3_staging_dir", S3_STAGING_DIR);
        info.put("log_path", "noctua.log");
        info.put("aws_credentials_provider_class","com.amazonaws.auth.DefaultAWSCredentialsProviderChain");

        return DriverManager.getConnection(ATHENA_URL, info);
    }

}
