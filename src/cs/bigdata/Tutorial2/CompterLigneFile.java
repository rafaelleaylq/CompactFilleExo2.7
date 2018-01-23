package cs.bigdata.Tutorial2;


import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;



public class CompterLigneFile {

	private static BufferedReader br;

	public static void main(String[] args) throws IOException {
		
		int c = 0 ;
		String localSrc = "/home/cloudera/Downloads/arbres.csv";
		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		
		try{
			InputStreamReader isr = new InputStreamReader(in);
			br = new BufferedReader(isr);
			
			// read line by line
			String line = br.readLine();
			
			while (line !=null){
				// Process of the current line
				//System.out.println(line);
				// go to the next line
				line = br.readLine();
				c = c + 1 ;
			}
			
			System.out.println(c) ;
		}
		finally{
			//close the file
			in.close();
			fs.close();
		}
 
		
		
	}

}
