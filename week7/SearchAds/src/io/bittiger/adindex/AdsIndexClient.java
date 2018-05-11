package io.bittiger.adindex;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.List;
import java.util.ArrayList;

public class AdsIndexClient {
    private static final Logger logger = Logger.getLogger(AdsIndexClient.class.getName());

    private final ManagedChannel channel;
    private final AdsIndexGrpc.AdsIndexBlockingStub blockingStub;
    public AdsIndexClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port)
                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
                // needing certificates.
                .usePlaintext(true));
    }

    /** Construct client for accessing RouteGuide server using the existing channel. */
    AdsIndexClient(ManagedChannelBuilder<?> channelBuilder) {
        channel = channelBuilder.build();
        blockingStub = AdsIndexGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }
    
    public java.util.List<io.bittiger.adindex.Ad> GetAds(java.util.List<io.bittiger.adindex.Query> queryList,String device_id, String device_ip) {
    	AdsRequest.Builder request = AdsRequest.newBuilder();
		System.out.println("queryList.size() : " + queryList.size());
    	for(int i = 0; i < queryList.size();i++) {
    		io.bittiger.adindex.Query q = queryList.get(i);
    		System.out.println("q.getTermCount() : " + q.getTermCount());
    		for(int index = 0; index < q.getTermCount();index++) {
    			System.out.println("preparing request term : " + q.getTerm(index));
    		}
        	request.addQuery(q);
    	}
    	request.setDeviceId(device_id);
    	request.setDeviceIp(device_ip);
    	AdsReply reply;
    	try {
			System.out.println("sending request...");
        	reply = blockingStub.getAds(request.build());
         	List<io.bittiger.adindex.Ad> adList = new ArrayList<io.bittiger.adindex.Ad>();
        	adList = reply.getAdList();  	
    		return adList;  
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
        }
    	return null;
    }
}
