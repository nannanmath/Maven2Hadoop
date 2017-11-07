package nan.learnjava.maven.TestMave2Eclipse;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.net.DNSToSwitchMapping;

public class RackAware implements DNSToSwitchMapping {

	public List<String> resolve(List<String> names) {
		List<String> rackID = new ArrayList<String>();
		for (String name : names) {
			// Get IP.
			Integer ip = null;
			if (name.startsWith("192")) {
				ip = Integer.parseInt(name.substring(name.lastIndexOf(".") + 1));
			} else {
				ip = Integer.parseInt(name.substring(1));
			}
			// Set rack id as String.
			if (ip <= 203) {
				rackID.add("/rack1/s" + ip);
			} else {
				rackID.add("/rack2/s" + ip);
			}
		}
		return rackID;
	}

	public void reloadCachedMappings() {
	}

	public void reloadCachedMappings(List<String> names) {
	}
}
