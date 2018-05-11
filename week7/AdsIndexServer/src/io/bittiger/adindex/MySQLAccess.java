package io.bittiger.adindex;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
/**
 * Created by jiayangan on 5/14/17.
 */


public class MySQLAccess {
    private Connection d_connect = null;
    private String d_user_name;
    private String d_password;
    private String d_server_name;
    private String d_db_name;
    public void close() throws Exception {
        System.out.println("Close database");
        try {
            if (d_connect != null) {
                d_connect.close();
            }
        } catch (Exception e) {
            throw e;
        }
    }

    public MySQLAccess(String server,String db,String user,String password) {
        d_server_name = server;
        d_db_name = db;
        d_user_name = user;
        d_password = password;
    }

    private Connection getConnection() throws Exception {
        try {
            // This will load the MySQL driver, each DB has its own driver
            Class.forName("com.mysql.jdbc.Driver");
            String conn = "jdbc:mysql://" + d_server_name + "/" +
                    d_db_name+"?user="+d_user_name+"&password="+d_password;
            System.out.println("Connecting to database: " + conn);
            d_connect = DriverManager.getConnection(conn);
            System.out.println("Connected to database");
            return d_connect;
        } catch(Exception e) {
            throw e;
        }
    }

    private Boolean isRecordExist(Connection connect,String sql_string) throws SQLException {
        PreparedStatement existStatement = null;
        boolean isExist = false;

        try
        {
            existStatement = connect.prepareStatement(sql_string);
            ResultSet result_set = existStatement.executeQuery();
            if (result_set.next())
            {
                isExist = true;
            }
        }
        catch(SQLException e )
        {
            System.out.println(e.getMessage());
            throw e;
        }
        finally
        {
            if (existStatement != null)
            {
                existStatement.close();
            };
        }

        return isExist;
    }


    public Ad.Builder getAdData(Long adId) throws Exception {
        Connection connect = null;
        PreparedStatement adStatement = null;
        ResultSet result_set = null;
        Ad.Builder ad = Ad.newBuilder();
        String sql_string = "select * from " + d_db_name + ".ad where adId=" + adId;
        try {
            connect = getConnection();
            adStatement = connect.prepareStatement(sql_string);
            result_set = adStatement.executeQuery();
            while (result_set.next()) {
                ad.setAdId(result_set.getLong("adId"));
                ad.setCampaignId(result_set.getLong("campaignId"));
                String keyWords = result_set.getString("keyWords");
                String[] keyWordsList = keyWords.split(",");
                for(int index = 0; index < keyWordsList.length; index++) {
                    ad.addKeyWords(keyWordsList[index]);
                }
                ad.setBidPrice(result_set.getDouble("bidPrice"));
                ad.setPrice(result_set.getDouble("price"));
                ad.setThumbnail(result_set.getString("thumbnail"));
                ad.setDescription(result_set.getString("description"));
                ad.setBrand(result_set.getString("brand"));
                ad.setDetailUrl(result_set.getString("detail_url"));
                ad.setCategory(result_set.getString("category"));
                ad.setTitle(result_set.getString("title"));
            }
        }
        catch(SQLException e )
        {
            System.out.println(e.getMessage());
            throw e;
        }
        finally
        {
            if (adStatement != null) {
                adStatement.close();
            };
            if (result_set != null) {
                result_set.close();
            }
            if (connect != null) {
                connect.close();
            }
        }
        return ad;
    }


}
