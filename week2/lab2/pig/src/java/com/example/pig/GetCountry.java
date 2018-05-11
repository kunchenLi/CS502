package com.example.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;

public class GetCountry extends EvalFunc<String> {
    LookupService cl;
	@Override
	public String exec(Tuple t) throws IOException {
	    if (cl == null) {
	        cl = new LookupService("GeoLiteCity.dat",
	                LookupService.GEOIP_MEMORY_CACHE );
	    }
	    Location loc = cl.getLocation((String)t.get(0));
	    if (loc == null) {
	        return null;
	    }
	    return loc.countryCode;
	}
	@Override
    public List<String> getShipFiles() {
        List<String> shipFiles = new ArrayList<String>();
        shipFiles.add("GeoLiteCity.dat");
        return shipFiles;
    }
}