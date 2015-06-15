package gr.ntua.cslab.streamnet.beans;

import java.util.ArrayList;

public class SflowsList {
private ArrayList<String> sflowsList;
	
	public SflowsList() {
		this.sflowsList = new ArrayList<String>();
	}

	public SflowsList(ArrayList<String> sflowsList) {
		super();
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
