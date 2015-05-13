package gr.ntua.cslab.datixSlave.beans;

import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class CachedSflowsList {
	private Map<String, SflowsList> cachedSflows;

	public CachedSflowsList() {
	}
	
	public CachedSflowsList(Map<String, SflowsList> cachedSflows) {
		super();
		this.cachedSflows = cachedSflows;
	}

	public Map<String, SflowsList> getCachedSflows() {
		return cachedSflows;
	}

	public void setCachedSflows(Map<String, SflowsList> cachedSflows) {
		this.cachedSflows = cachedSflows;
	}
}
