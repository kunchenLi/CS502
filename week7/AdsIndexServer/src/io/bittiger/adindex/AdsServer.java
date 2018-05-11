package io.bittiger.adindex;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.logging.Logger;
/**
 * Created by jiayangan on 5/14/17.
 */
public class AdsServer {
    private static final Logger logger = Logger.getLogger(AdsServer.class.getName());
    private Server server;
    private int port;
    private String mMemcachedServer;
    private int mMemcachedPortal;
    private int mTFMemcachedPortal;
    private int mDFMemcachedPortal;
    private int mClickFeaturePortal;

    private String mysql_host;
    private String mysql_db;
    private String mysql_user;
    private String mysql_pass;
    private String m_logistic_reg_model_file;
    private String m_gbdt_model_path;
    public  AdsServer(int _port, String _memcachedServer,
                      int _memcachedPortal,
                      int _tfMemcachedPortal,
                      int _dfMemcachedPortal,
                      int _clickMemcachedPortal,
                      String _logistic_reg_model_file,
                      String _gbdt_model_path,
                      String _mysql_host,
                      String _mysql_db, String _mysql_user,
                      String _mysql_pass) {
        port = _port;
        mMemcachedServer = _memcachedServer;
        mMemcachedPortal = _memcachedPortal;
        mTFMemcachedPortal = _tfMemcachedPortal;
        mDFMemcachedPortal = _dfMemcachedPortal;
        mClickFeaturePortal = _clickMemcachedPortal;
        m_logistic_reg_model_file = _logistic_reg_model_file;
        m_gbdt_model_path = _gbdt_model_path;
        mysql_host = _mysql_host;
        mysql_db = _mysql_db;
        mysql_user = _mysql_user;
        mysql_pass = _mysql_pass;
    }

    private void start() throws IOException {
    /* The port on which the server should run */
        server = ServerBuilder.forPort(port)
                .addService(new AdsIndexServerImpl(mMemcachedServer, mMemcachedPortal,
                        mTFMemcachedPortal, mDFMemcachedPortal,mClickFeaturePortal, m_logistic_reg_model_file,m_gbdt_model_path,
                        mysql_host, mysql_db,
                        mysql_user, mysql_pass))
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                //clean up. release some resource: mysql connection, close file, close memcached client
                AdsServer.this.stop();
                System.err.println("ads index server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    /**
     * Main launches the server from the command line.
     * java -jar AdsIndexServer.jar 50051 127.0.0.1 11212 11220  11221 127.0.0.1:3306 searchads root bittiger2017
     * java -jar AdsIndexServer.jar 50052 127.0.0.1 11211 11220  11221 127.0.0.1:3306 searchads root bittiger2017
     * java -jar AdsIndexServer.jar 50053 127.0.0.1 11213 11220  11221 127.0.0.1:3306 searchads root bittiger2017
     * java -jar AdsIndexServer.jar 50054 127.0.0.1 11214 11220  11221 127.0.0.1:3306 searchads root bittiger2017
     * java -jar AdsIndexServer.jar 50055 127.0.0.1 11215 11220  11221 127.0.0.1:3306 searchads root bittiger2017
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        String portStr = args[0];
        int port = Integer.parseInt(portStr);
        String  memcachedServer = args[1];
        int memcachedPortal = Integer.parseInt(args[2]);
        int tfMemcachedPortal = Integer.parseInt(args[3]);
        int dfMemcachedPortal = Integer.parseInt(args[4]);
        String mysql_host = args[5];
        String mysql_db = args[6];
        String mysql_user = args[7];
        String mysql_pass = args[8];
        String logistic_reg_model_file = "/Users/jiayangan/project/SearchAds/data/model/ctrLogisticRegression.txt";
        String gbdt_model_path = "";
        int clickMemcachedPortal = 11218;

        final AdsServer server = new AdsServer(port, memcachedServer,
                memcachedPortal, tfMemcachedPortal, dfMemcachedPortal,clickMemcachedPortal, logistic_reg_model_file, gbdt_model_path, mysql_host, mysql_db,
                mysql_user, mysql_pass);
        server.start();
        server.blockUntilShutdown();
    }





}
