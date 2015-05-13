package gr.ntua.cslab.datixSlave.beans;

import java.util.ArrayList;
import java.util.Arrays;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class SflowsList {
	private ArrayList<String> sflowsList;
	
	public SflowsList() {
		this.sflowsList = new ArrayList<String>(Arrays.asList("a", "b", "c"));
	}

	public SflowsList(ArrayList<String> sflowsList) {
		this.sflowsList = sflowsList;
	}

	public ArrayList<String> getSflowsList() {
		return sflowsList;
	}

	public void setSflowsList(ArrayList<String> sflowsList) {
		this.sflowsList = sflowsList;
	}
	
	public void updateList(String input) {
		this.sflowsList.add(input);
	}
}
