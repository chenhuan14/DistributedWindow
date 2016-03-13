package nudt.pdl.stormwindow;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

import nudt.pdl.stormwindow.util.Constant;

public class CreateRandomLongToFile {
	

	public static Long Max = 1000L;

	public static void main(String[] args)  {
		
		Random random = new Random();
		File outPutFile = new File(Constant.LONG_FILE_10K);
		if(!outPutFile.exists())
			try {
				outPutFile.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
		FileWriter writer = null;
		try {
			writer = new FileWriter(outPutFile, true);
			
			for(int i = 0 ;i < 10000; i++)
			{
				long l = Math.abs(random.nextLong() % Max);
				writer.write(l+"\n");
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			if(writer != null)
			{
				try {
					writer.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	
	}

}
