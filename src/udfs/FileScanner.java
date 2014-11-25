package diplom;

import java.io.BufferedReader;
import java.io.IOException;

public class FileScanner {
private BufferedReader reader = null;
	
	public FileScanner( BufferedReader reader) {
		this.reader = reader;
	}
	
	public String next() throws IOException{
		return reader.readLine();
	}
	
	public String seekTo(String key) throws IOException {
		
		String result = reader.readLine();
		while (result != null && result.compareTo(key) < 0){
			result = reader.readLine();
		}
		return result;
	}
	
	public void close() throws IOException {
		reader.close();
	}
}
