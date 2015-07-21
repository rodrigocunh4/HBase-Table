package sensors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseSensors {

	private static Configuration conf = null;
	/**
	 * Initialization
	 */
	static {
		conf = HBaseConfiguration.create();
	}

	/**
	 * Create a table
	 */
	@SuppressWarnings("deprecation")
	public static void creatTable(String tableName, String[] familys)
			throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName)) {
			System.out.println("table already exists!");
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			for (int i = 0; i < familys.length; i++) {
				tableDesc.addFamily(new HColumnDescriptor(familys[i]));
			}
			admin.createTable(tableDesc);
			System.out.println("create table " + tableName + " ok.");
		}
		admin.close();
	}

	/**
	 * Delete a table
	 */
	@SuppressWarnings("deprecation")
	public static void deleteTable(String tableName) throws Exception {
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			System.out.println("delete table " + tableName + " ok.");
			admin.close();
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Put (or insert) a row
	 */
	@SuppressWarnings("deprecation")
	public static void addRecord(String tableName, String rowKey,
			String family, String qualifier, String value) throws Exception {
		try {
			HTable table = new HTable(conf, tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier),
					Bytes.toBytes(value));
			table.put(put);
			System.out.println("insert recored " + rowKey + " to table "
					+ tableName + " ok.");
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Delete a row
	 */
	@SuppressWarnings("deprecation")
	public static void delRecord(String tableName, String rowKey)
			throws IOException {
		HTable table = new HTable(conf, tableName);
		List<Delete> list = new ArrayList<Delete>();
		Delete del = new Delete(rowKey.getBytes());
		list.add(del);
		table.delete(list);
		System.out.println("del recored " + rowKey + " ok.");
		table.close();
	}

	/**
	 * Get a row
	 */
	@SuppressWarnings("deprecation")
	public static void getOneRecord(String tableName, String rowKey)
			throws IOException {
		HTable table = new HTable(conf, tableName);
		Get get = new Get(rowKey.getBytes());
		Result rs = table.get(get);
		for (KeyValue kv : rs.raw()) {
			System.out.print(new String(kv.getRow()) + " ");
			System.out.print(new String(kv.getFamily()) + ":");
			System.out.print(new String(kv.getQualifier()) + " ");
			System.out.print(kv.getTimestamp() + " ");
			System.out.println(new String(kv.getValue()));
		}
		table.close();
	}

	/**
	 * Scan (or list) a table
	 */
	@SuppressWarnings("deprecation")
	public static void getAllRecord(String tableName) {
		try {
			HTable table = new HTable(conf, tableName);
			Scan s = new Scan();
			ResultScanner ss = table.getScanner(s);
			for (Result r : ss) {
				for (KeyValue kv : r.raw()) {
					System.out.print(new String(kv.getRow()) + " ");
					System.out.print(new String(kv.getFamily()) + ":");
					System.out.print(new String(kv.getQualifier()) + " ");
					System.out.print(kv.getTimestamp() + " ");
					System.out.println(new String(kv.getValue()));
				}
			}
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Return number of rows
	 */
	@SuppressWarnings("deprecation")
	public static int getTotalRows(String tableName) {
		int totalRows = 0;
		try {
			HTable table = new HTable(conf, tableName);
			Scan s = new Scan();
			ResultScanner ss = table.getScanner(s);
			for (Result r : ss) {
				totalRows++;
			}
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return totalRows;
	}

	/**
	 * Calculate average of a given data in a given range of time
	 */
	public static void average(String tableName, String initialDate,
			String finalDate, String data) throws IOException {
		try {
			HTable table = new HTable(conf, tableName);
			Scan s = new Scan();
			ResultScanner ss = table.getScanner(s);

			// Variables to calculate average
			float average = 0;
			float sum = 0;
			int count = 0;

			// Variables for the range
			int initYear = Integer.parseInt(initialDate.substring(0, 4));
			int initMonth = Integer.parseInt(initialDate.substring(4, 6));
			int initDay = Integer.parseInt(initialDate.substring(6, 8));
			int initHour = Integer.parseInt(initialDate.substring(8, 10));
			int initMin = Integer.parseInt(initialDate.substring(10, 12));
			String initDayLight = initialDate.substring(12);

			String initHourStr = initialDate.substring(8, 10);
			String initMinStr = initialDate.substring(10, 12);

			int finalYear = Integer.parseInt(finalDate.substring(0, 4));
			int finalMonth = Integer.parseInt(finalDate.substring(4, 6));
			int finalDay = Integer.parseInt(finalDate.substring(6, 8));
			int finalHour = Integer.parseInt(finalDate.substring(8, 10));
			int finalMin = Integer.parseInt(finalDate.substring(10, 12));
			String finalDayLight = finalDate.substring(12);

			String finalHourStr = finalDate.substring(8, 10);
			String finalMinStr = finalDate.substring(10, 12);

			String time = null;
			String dataValue = null;

			for (int i = 1; i < HBaseSensors.getTotalRows(tableName) + 1; i++) {
				Get get = new Get(String.valueOf(i).getBytes());
				Result rs = table.get(get);
				for (KeyValue kv : rs.raw()) {

					if (new String(kv.getQualifier()).equals("time"))
						time = new String(kv.getValue());

					else if (new String(kv.getQualifier()).equals(data)) {
						dataValue = new String(kv.getValue());
					}
				}

				int year = Integer.parseInt(time.substring(0, 4));
				int month = Integer.parseInt(time.substring(4, 6));
				int day = Integer.parseInt(time.substring(6, 8));
				int hour = Integer.parseInt(time.substring(8, 10));
				int min = Integer.parseInt(time.substring(10, 12));
				String dayLight = time.substring(12);

				// Check if the value is in the range
				if (year == finalYear && month == finalMonth && day == finalDay) {
					if (dayLight.equals("am") && finalDayLight.equals("pm")) {
						System.out.println(Integer.parseInt(dataValue));
						sum = sum + Integer.parseInt(dataValue);
						count++;
					} else if (dayLight.equals(finalDayLight)
							&& hour <= finalHour && min <= finalMin) {
						System.out.println(Integer.parseInt(dataValue));
						sum = sum + Integer.parseInt(dataValue);
						count++;
					}
				} else if (year >= initYear && year <= finalYear)
					if (month >= initMonth && month <= finalMonth)
						if (day >= initDay && day <= finalDay)
							if (hour >= initHour && hour <= finalHour)
								if (min >= initMin && min <= finalMin) {
									System.out.println(Integer
											.parseInt(dataValue));
									sum = sum + Integer.parseInt(dataValue);
									count++;
								}

			}

			average = sum / count;
			
			//Print result
			System.out.println("Average of " + data + " in " + tableName
					+ " is " + average);
			System.out.println("From: " + initMonth + "/" + initDay + "/"
					+ initYear + " " + initHourStr + ":" + initMinStr
					+ initDayLight + " to " + finalMonth + "/" + finalDay + "/"
					+ finalYear + " " + finalHourStr + ":" + finalMinStr
					+ finalDayLight);

			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Calculate standard deviation of a given data in a given range of time
	 */
	public static void standardDeviation(String tableName, String initialDate,
			String finalDate, String data) throws IOException {
		try {
			HTable table = new HTable(conf, tableName);
			Scan s = new Scan();
			ResultScanner ss = table.getScanner(s);

			// Variables to calculate standard deviation
			float average = 0;
			float sum = 0;
			int count = 0;
			double deviation = 0;
			double standardDeviation = 0;
			ArrayList<Integer> dataList = new ArrayList<Integer>();

			// Variables for the range
			int initYear = Integer.parseInt(initialDate.substring(0, 4));
			int initMonth = Integer.parseInt(initialDate.substring(4, 6));
			int initDay = Integer.parseInt(initialDate.substring(6, 8));
			int initHour = Integer.parseInt(initialDate.substring(8, 10));
			int initMin = Integer.parseInt(initialDate.substring(10, 12));
			String initDayLight = initialDate.substring(12);

			String initHourStr = initialDate.substring(8, 10);
			String initMinStr = initialDate.substring(10, 12);

			int finalYear = Integer.parseInt(finalDate.substring(0, 4));
			int finalMonth = Integer.parseInt(finalDate.substring(4, 6));
			int finalDay = Integer.parseInt(finalDate.substring(6, 8));
			int finalHour = Integer.parseInt(finalDate.substring(8, 10));
			int finalMin = Integer.parseInt(finalDate.substring(10, 12));
			String finalDayLight = finalDate.substring(12);

			String finalHourStr = finalDate.substring(8, 10);
			String finalMinStr = finalDate.substring(10, 12);

			String time = null;
			String dataValue = null;

			for (int i = 1; i < HBaseSensors.getTotalRows(tableName) + 1; i++) {
				Get get = new Get(String.valueOf(i).getBytes());
				Result rs = table.get(get);
				for (KeyValue kv : rs.raw()) {

					if (new String(kv.getQualifier()).equals("time"))
						time = new String(kv.getValue());

					else if (new String(kv.getQualifier()).equals(data))
						dataValue = new String(kv.getValue());
				}

				int year = Integer.parseInt(time.substring(0, 4));
				int month = Integer.parseInt(time.substring(4, 6));
				int day = Integer.parseInt(time.substring(6, 8));
				int hour = Integer.parseInt(time.substring(8, 10));
				int min = Integer.parseInt(time.substring(10, 12));
				String dayLight = time.substring(12);

				// Check if the value is in the range
				if (year == finalYear && month == finalMonth && day == finalDay) {
					if (dayLight.equals("am") && finalDayLight.equals("pm")) {
						sum = sum + Integer.parseInt(dataValue);
						dataList.add(Integer.parseInt(dataValue));
						count++;
					} else if (dayLight.equals(finalDayLight)
							&& hour <= finalHour && min <= finalMin) {
						sum = sum + Integer.parseInt(dataValue);
						dataList.add(Integer.parseInt(dataValue));
						count++;
					}
				} else if (year >= initYear && year <= finalYear)
					if (month >= initMonth && month <= finalMonth)
						if (day >= initDay && day <= finalDay)
							if (hour >= initHour && hour <= finalHour)
								if (min >= initMin && min <= finalMin) {
									sum = sum + Integer.parseInt(dataValue);
									dataList.add(Integer.parseInt(dataValue));
									count++;
								}

			}

			average = sum / count;

			// Calculate deviation
			for (int i = 0; i < count; i++) {
				System.out.println(dataList.get(i));
				deviation = deviation + Math.pow(dataList.get(i) - average, 2);
			}

			deviation = deviation / count;

			standardDeviation = Math.sqrt(deviation);

			
			//Print result
			System.out.println("Standard deviation of " + data + " in "
					+ tableName + " is " + standardDeviation);
			
			System.out.println("From: " + initMonth + "/" + initDay + "/"
					+ initYear + " " + initHourStr + ":" + initMinStr
					+ initDayLight + " to " + finalMonth + "/" + finalDay + "/"
					+ finalYear + " " + finalHourStr + ":" + finalMinStr
					+ finalDayLight);

			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] agrs) {
		try {

			HBaseSensors.average("sensor-1", "201507010100am",
					"201507010300am", "temperature");
			HBaseSensors.standardDeviation("sensor-1", "201507010100am",
					"201507010300am", "temperature");

			/**
			 * Create and populate table
			 * 
			 * // Create table "sensor-1"; String tableSensor1 = "sensor-1";
			 * String[] sensor1Family = {"sensorData1"};
			 * HBaseSensors.creatTable(tableSensor1, sensor1Family);
			 * 
			 * 
			 * int row = 0; int temperature = 0; int humidity = 0; int light =
			 * 0; String nutrition = "10:20:30";
			 * 
			 * Random generator = new Random();
			 * 
			 * HBaseSensors.addRecord(tableSensor1, "1", "sensorData1", "time",
			 * "201507010100am"); HBaseSensors.addRecord(tableSensor1, "2",
			 * "sensorData1", "time", "201507010300am");
			 * HBaseSensors.addRecord(tableSensor1, "3", "sensorData1", "time",
			 * "201507010500am"); HBaseSensors.addRecord(tableSensor1, "4",
			 * "sensorData1", "time", "201507010700am");
			 * 
			 * for (int z = 0; z < 4; z++) { row++; temperature = 50 +
			 * generator.nextInt(30); humidity = 50 + generator.nextInt(30);
			 * light = 50 + generator.nextInt(30);
			 * 
			 * HBaseSensors.addRecord(tableSensor1, Integer.toString(row),
			 * "sensorData1", "temperature", Integer.toString(temperature));
			 * HBaseSensors.addRecord(tableSensor1, Integer.toString(row),
			 * "sensorData1", "humidity", Integer.toString(humidity));
			 * HBaseSensors.addRecord(tableSensor1, Integer.toString(row),
			 * "sensorData1", "light", Integer.toString(light));
			 * HBaseSensors.addRecord(tableSensor1, Integer.toString(row),
			 * "sensorData1", "nutrition", nutrition); }
			 **/

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
