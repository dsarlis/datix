package gr.ntua.cslab.datixSlave.beans;

import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class SflowsToStoreList {
	private  Map<String, SflowsList> sflowsToStore ;

	public SflowsToStoreList() {
		this.sflowsToStore = new HashMap<String, SflowsList>();
		this.sflowsToStore.put("apple", new SflowsList());
		this.sflowsToStore.put("banana", new SflowsList());
		this.sflowsToStore.put("carrot", new SflowsList());
	}
	
	public SflowsToStoreList(Map<String, SflowsList> sflowsToStore) {
		super();
		this.sflowsToStore = sflowsToStore;
	}

	public Map<String, SflowsList> getSflowsToStore() {
		return sflowsToStore;
	}

	public void setSflowsToStore(Map<String, SflowsList> sflowsToStore) {
		this.sflowsToStore = sflowsToStore;
	}
}
