package com.ibeifeng.hive.udf;

import java.text.SimpleDateFormat;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;


public class DateTransUdf extends UDF {

	/**
	 * 日期装换
	 * @param datetime  实际日期格式       
	 * @param transdate 希望转成的日期格式   
	 * @return
	 */
	public Text evaluate(Text datetime,Text transdate){
		try {
			SimpleDateFormat sdfstart= null;
			SimpleDateFormat sdfend = new SimpleDateFormat(transdate.toString());
			//yyyy-MM-dd HH:mm:ss
			Pattern p1 = Pattern.compile(
					"^((([0-9][0-9][0-9][0-9]-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01]))|(20[0-3][0-9]-(0[2469]|11)-(0[1-9]|[12][0-9]|30))) (20|21|22|23|[0-1][0-9]):[0-5][0-9]:[0-5][0-9])$");
			//yyyy/MM/dd HH:mm:ss
			Pattern p2 = Pattern.compile(
					"^((([0-9][0-9][0-9][0-9]/(0[1-9]|1[0-2])/(0[1-9]|[12][0-9]|3[01]))|(20[0-3][0-9]-(0[2469]|11)-(0[1-9]|[12][0-9]|30))) (20|21|22|23|[0-1][0-9]):[0-5][0-9]:[0-5][0-9])$");
			//yyyyMMdd HH:mm:ss
			Pattern p3 = Pattern.compile(
					"^((([0-9][0-9][0-9][0-9](0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01]))|(20[0-3][0-9]-(0[2469]|11)-(0[1-9]|[12][0-9]|30))) (20|21|22|23|[0-1][0-9]):[0-5][0-9]:[0-5][0-9])$");
			//yyyy-MM-dd
			Pattern p4 = Pattern.compile(
					"^(([0-9][0-9][0-9][0-9]-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01]))|(20[0-3][0-9]-(0[2469]|11)-(0[1-9]|[12][0-9]|30)))");
			//yyyy/MM/dd
			Pattern p5 = Pattern.compile(
					"^(([0-9][0-9][0-9][0-9]/(0[1-9]|1[0-2])/(0[1-9]|[12][0-9]|3[01]))|(20[0-3][0-9]-(0[2469]|11)-(0[1-9]|[12][0-9]|30)))");
			//yyyyMMdd
			Pattern p6 = Pattern.compile(
					"^(([0-9][0-9][0-9][0-9](0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01]))|(20[0-3][0-9]-(0[2469]|11)-(0[1-9]|[12][0-9]|30)))");
			//yyyyMMddHHmmss
			Pattern p7 = Pattern.compile(
					"^((([0-9][0-9][0-9][0-9](0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01]))|(20[0-3][0-9]-(0[2469]|11)-(0[1-9]|[12][0-9]|30)))(20|21|22|23|[0-1][0-9])[0-5][0-9][0-5][0-9])$");
			//yyyyMMdd HHmmss
			Pattern p8 = Pattern.compile(
					"^((([0-9][0-9][0-9][0-9](0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01]))|(20[0-3][0-9]-(0[2469]|11)-(0[1-9]|[12][0-9]|30))) (20|21|22|23|[0-1][0-9])[0-5][0-9][0-5][0-9])$");
			if(p1.matcher(datetime.toString()).matches()){
				sdfstart = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");						
			}else if(p2.matcher(datetime.toString()).matches()){
				sdfstart = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");	
			}else if(p3.matcher(datetime.toString()).matches()){
				sdfstart = new SimpleDateFormat("yyyyMMdd HH:mm:ss");	
			}else if(p4.matcher(datetime.toString()).matches()){
				sdfstart = new SimpleDateFormat("yyyy-MM-dd");	
			}else if(p5.matcher(datetime.toString()).matches()){
				sdfstart = new SimpleDateFormat("yyyy/MM/dd");	
			}else if(p6.matcher(datetime.toString()).matches()){
				sdfstart = new SimpleDateFormat("yyyyMMdd");	
			}else if(p7.matcher(datetime.toString()).matches()){
				sdfstart = new SimpleDateFormat("yyyyMMddHHmmss");	
			}else if(p8.matcher(datetime.toString()).matches()){
				sdfstart = new SimpleDateFormat("yyyyMMdd HHmmss");	
			}else{
				throw new Exception(datetime+"无法装换的时间类型!");
			}
            return new Text(sdfend.format(sdfstart.parse(datetime.toString())));
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
    }
	
	
	public static void main(String[] args) {
		DateTransUdf dtu = new DateTransUdf();
		Text text1 = new Text("20161031");
		Text text2 = new Text("yyyyMMdd");
		System.out.println(dtu.evaluate(text1,text2));
	}

}
