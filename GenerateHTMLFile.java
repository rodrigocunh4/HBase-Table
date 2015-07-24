package sensors;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class GenerateHTMLFile {
	
	/**
	 * Generate HTML file
	 * @param resultType
	 * @param data
	 * @param tableName
	 * @param result
	 * @param initDate
	 * @param finalDate
	 * @throws IOException
	 */
	public static void generateHTML(String resultType, String data, String tableName, double result, String initDate, String finalDate) throws IOException
	{
		
		
		int initYear = Integer.parseInt(initDate.substring(0, 4));
		int initMonth = Integer.parseInt(initDate.substring(4, 6));
		int initDay = Integer.parseInt(initDate.substring(6, 8));
		String initHourStr = initDate.substring(8, 10);
		String initMinStr = initDate.substring(10, 12);
		String initDayLight = initDate.substring(12);

		int finalYear = Integer.parseInt(finalDate.substring(0, 4));
		int finalMonth = Integer.parseInt(finalDate.substring(4, 6));
		int finalDay = Integer.parseInt(finalDate.substring(6, 8));
		String finalHourStr = finalDate.substring(8, 10);
		String finalMinStr = finalDate.substring(10, 12);
		String finalDayLight = finalDate.substring(12);
		
		data = Character.toUpperCase(data.charAt(0)) + data.substring(1);
		
		StringBuilder sb = new StringBuilder();
		sb.append("<!DOCTYPE html>");
		sb.append("<html lang=\"en\">");
		sb.append("<head><title>"+data+" results</title></head>");
		sb.append("<body><h3>"+resultType+" "+data+"</h3>");
		sb.append("<p>"+resultType+" of "+data+" in "+tableName+" is "+result+"</p>");
		sb.append("<p> From: "+ initMonth + "/" + initDay + "/"
					+ initYear + " " + initHourStr + ":" + initMinStr
					+ initDayLight + " to " + finalMonth + "/" + finalDay + "/"
					+ finalYear + " " + finalHourStr + ":" + finalMinStr
					+ finalDayLight+"</p>");
		sb.append("</body>");
		sb.append("</html>");
	    FileWriter fstream = new FileWriter(resultType+data+initDate+finalDate+".html");
	    BufferedWriter out = new BufferedWriter(fstream);
	    out.write(sb.toString());
	    out.close();
	}
}
