package io.bittiger.adindex;
import java.util.List;

/**
 * Created by jiayangan on 5/14/17.
 */
public class AdsIndexServerImpl extends AdsIndexGrpc.AdsIndexImplBase{
    private String mMemcachedServer;
    private int mMemcachedPortal;
    private int mTFMemcachedPortal;
    private int mDFMemcachedPortal;
    private int mClickFeaturePortal;
    private String m_logistic_reg_model_file;
    private String m_gbdt_model_path;
    private String mysql_host;
    private String mysql_db;
    private String mysql_user;
    private String mysql_pass;
    public AdsIndexServerImpl(String memcachedServer,int memcachedPortal,int tfMemcachedPortal, int dfMemcachedPortal, int clickMemcachedPortal, String logistic_reg_model_file,
                              String gbdt_model_path, String mysqlHost,String mysqlDb,String user,String pass) {
        mMemcachedServer = memcachedServer;
        mMemcachedPortal = memcachedPortal;
        mTFMemcachedPortal = tfMemcachedPortal;
        mDFMemcachedPortal = dfMemcachedPortal;
        mClickFeaturePortal = clickMemcachedPortal;
        m_logistic_reg_model_file = logistic_reg_model_file;
        m_gbdt_model_path = gbdt_model_path;
        mysql_host = mysqlHost;
        mysql_db = mysqlDb;
        mysql_user = user;
        mysql_pass = pass;
    }
    @Override
    public void getAds(io.bittiger.adindex.AdsRequest request,
                       io.grpc.stub.StreamObserver<io.bittiger.adindex.AdsReply> responseObserver) {
        System.out.println("received requests number of query:" + request.getQueryCount());
        //#3: concurrent call selectAds for all query
        for(int i = 0; i < request.getQueryCount();i++) {
            Query query = request.getQuery(i);

            List<Ad>  adsCandidates = AdsSelector.
                    getInstance(mMemcachedServer, mMemcachedPortal,mTFMemcachedPortal,
                            mDFMemcachedPortal,mClickFeaturePortal,m_logistic_reg_model_file,
                            m_gbdt_model_path, mysql_host, mysql_db,mysql_user, mysql_pass).
                    selectAds(query, request.getDeviceId(), request.getDeviceIp());
            //design choice
            //#1 : send response back for each query immediately =>
            //pro: client in web server don't have to wait too long, can at least receive some ads,
            //cons: may not get complete list of ads, de-dupe and sort on client side (ads web server)
            //#2 : aggregate ads for each query on server, then send them back =>
            //pro: de-dupe, sort done on server, cons: timeout
            AdsReply.Builder replyBuilder = AdsReply.newBuilder();
            for(Ad ad : adsCandidates) {
                if(ad.getRelevanceScore() > 0.07 && ad.getPClick() > 0.001) {
                    replyBuilder.addAd(ad);
                }
            }
            AdsReply reply = replyBuilder.build();
            responseObserver.onNext(reply);
        }
        //#2: aggregate ads for each query then send to responseObserver
        //dedupe, sort
        //Request { nike running shoe: 50 ms, nike running sneaker: 250 ms}, 300 ms, client time out: 250 ms

        //#3, thread1: nike running shoe => 50 ms, thread 2: nike running sneaker: 250 ms, join: 250 ms, add time out for each thread
        responseObserver.onCompleted();
    }


}
