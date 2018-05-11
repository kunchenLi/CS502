package io.bittiger.adindex;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.List;

public class AdsIndexClientWorker extends Thread{
    private static final Logger logger = Logger.getLogger(AdsIndexClientWorker.class.getName());
    protected AdsSelectionResult result = null;
    private String adsIndexServer;
    private String deviceId;
    private String deviceIp;
    private int adsIndexServerPortal;
    private List<io.bittiger.adindex.Query> queryList;
    public AdsIndexClientWorker(List<io.bittiger.adindex.Query> queryList, String adsIndexServer,int adsIndexServerPortal, String deviceId, String deviceIp, AdsSelectionResult result) {
    	this.result = result;
    	this.queryList = queryList;
    	this.deviceId = deviceId;
    	this.deviceIp = deviceIp;
    	this.adsIndexServer = adsIndexServer;
    	this.adsIndexServerPortal = adsIndexServerPortal;
    }
    public void start()   {
    	io.bittiger.adindex.AdsIndexClient adsIndexClient = new io.bittiger.adindex.AdsIndexClient(adsIndexServer,adsIndexServerPortal);
        List<io.bittiger.adindex.Ad> adsList = adsIndexClient.GetAds(queryList,deviceId, deviceIp);
        result.add(adsList);
    }
}
