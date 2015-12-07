package main;

import help.DataRunner;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;

public class MainClass {
	private static final String HADOOP_IP="10.10.67.73";
	
	private static String getDataFileFromHadoop(String dealDate){
		DefaultExecutor executor = new DefaultExecutor();
		List<String> list = new ArrayList<String>();
		String  tarFileName = "/data/dw_tmp/dw_log_task_pool_src.sql";
		//删除本地日志文件目录
		File fileDir = new File("/data/dw_tmp/"+dealDate.replace("-", ""));
		list.add("rm -rf "+fileDir);
		//Hadoop日志文件目录传到本地
		StringBuilder sb = new StringBuilder("/data/dwetl/java/hadoop-2.7.1/bin/hadoop dfs -get hdfs://");
		sb.append(HADOOP_IP).append(":9000/user/flume/taskpoollog/").append(dealDate.replace("-", ""));
		sb.append("  /data/dw_tmp");
		
		System.out.println(sb.toString());
		
		list.add(sb.toString());
		sb.delete(0,sb.length());
		Iterator<String> it = list.iterator();
		
		while(it.hasNext()){
			try{
				String cmd = it.next();
				executor.execute(CommandLine.parse(cmd));
			}catch(Exception ex){
				ex.printStackTrace();
			}
		}
		
		if(fileDir.exists()){
			try{
				FileOutputStream fos = new FileOutputStream(tarFileName);
				FileInputStream fis = null;
				String[] files = fileDir.list();
				int flag = -1;
				for(String file : files){
					fis = new FileInputStream(fileDir+File.separator+file);
					while((flag = fis.read()) != -1){
						fos.write(flag);	
					}	
				}
				fos.flush();
				fos.close();
				fis.close();
			}catch(Exception ex){
				ex.printStackTrace();
			}
		}
		return tarFileName;
	}
	
	public static void main(String[] args) {
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.DAY_OF_MONTH, -1);
		String dealDate = new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime());
		if(args!=null&&args.length==1){
			dealDate = args[0];
			System.out.println("利用设置的参数--------------->:"+dealDate);
		}
		String srcFileName =  getDataFileFromHadoop(dealDate);
		DataRunner.getTarFileName(srcFileName,dealDate);
		
	}
}
