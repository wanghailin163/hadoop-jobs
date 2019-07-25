package com.wanghailin.ibeifeng.hive.udf;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * 获取该日期的 年/季/月/周 的第一天
 * @author WangHaiLin
 */
public class TimeGet extends UDF {

	
	/**
	 * @param datetime  实际日期格式                    yyyyMMdd
	 * @param transdate 转换的时间格式                yyyyMMdd
	 * @param timetype  时间类型                            Y/Q/M/W
	 * @return
	 */
	public Text evaluate(Text nowDate,Text transdate,Text timetype){
	    
	    Calendar cal = Calendar.getInstance();
	  
		//当前日期与当年的第一天的差值
		try {
			Date nowDateTime=new SimpleDateFormat("yyyyMMdd").parse(nowDate.toString());
			if("Y".equals(timetype.toString())){			
				cal.setTime(nowDateTime);
				int last = cal.getActualMinimum(Calendar.DAY_OF_YEAR); 
				cal.set(Calendar.DAY_OF_YEAR, last);  
				Date endDateTime=cal.getTime();
			
				return new Text(new SimpleDateFormat(transdate.toString()).format(endDateTime));
			}else if("Q".equals(timetype.toString())){
				cal.setTime(nowDateTime); 
			    int month = getQuarterInMonth(cal.get(Calendar.MONTH)+1, true);  
			    cal.set(Calendar.MONTH, month-1);  
			    cal.set(Calendar.DAY_OF_MONTH, 1);
			    Date endDateTime=cal.getTime();
			    
				return new Text(new SimpleDateFormat(transdate.toString()).format(endDateTime));
			}else if("M".equals(timetype.toString())){
				cal.setTime(nowDateTime);
				int last = cal.getActualMinimum(Calendar.DAY_OF_MONTH); 
				cal.set(Calendar.DAY_OF_MONTH, last);  
				Date endDateTime=cal.getTime();
				
				return new Text(new SimpleDateFormat(transdate.toString()).format(endDateTime));
			}else if("W".equals(timetype.toString())){
				cal.setTime(nowDateTime);
				int last = cal.getActualMinimum(Calendar.DAY_OF_WEEK); 
				cal.set(Calendar.DAY_OF_WEEK, last);  
				Date endDateTime=cal.getTime();

				return new Text(new SimpleDateFormat(transdate.toString()).format(endDateTime));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	/**
	 * @param month
	 * @param isQuarterStart true:季度初   flase:季度末   
	 * @return
	 */
	private static int getQuarterInMonth(int month, boolean isQuarterStart) {  
        int months[] = { 1, 4, 7, 10 };  
        if (!isQuarterStart) {  
            months = new int[] { 3, 6, 9, 12 };  
        }  
        if (month >= 1 && month <= 3)  
            return months[0];  
        else if (month >= 4 && month <= 6)  
            return months[1];  
        else if (month >= 7 && month <= 9)  
            return months[2];  
        else  
            return months[3];  
    }
	
	public static void main(String[] args) {
		TimeGet tg = new TimeGet();
		System.out.println(tg.evaluate(new Text("20180401"), new Text("yyyy-MM-dd"), new Text("Q")));
	}
}
