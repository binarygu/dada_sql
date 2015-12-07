package help;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.jdbc.support.rowset.SqlRowSet;

public class DataRunner {
	private static JdbcTemplate getJdbcTemplate(){
		JdbcTemplate jdbcTemplate = null;
		Properties properties = new Properties();
		try{
			InputStream is = new FileInputStream("/data/dwetl/config/offline_dw-master.properties");
        	properties.load(is);
        	String url = String.format("jdbc:mysql://%s:%s/temp?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull",
        			properties.getProperty("remote.ip"),
        			properties.getProperty("remote.port"));
        	DataSource dataSource = new SingleConnectionDataSource(url, properties.getProperty("remote.username"), properties.getProperty("remote.password"), true);
        	is.close();
        	jdbcTemplate = new JdbcTemplate(dataSource);
		}catch(Exception ex){
			ex.printStackTrace();
		}
		return jdbcTemplate;
	}

	
	private static Map<String, Integer> getCityCode(){
		JdbcTemplate jdbcTemplate = getJdbcTemplate();
		Map<String, Integer> map = new HashMap<String, Integer>();
		try{
			SqlRowSet rs = jdbcTemplate.queryForRowSet("select city_code,city_id from dim.dim_city;");
			while(rs.next()){
				map.put(rs.getString("city_code"),rs.getInt("city_id"));
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}
		return map;
	}
	
	
	private static Map<String, Integer> getLogTypeId(){
		Map<String, Integer> map = new HashMap<String, Integer>();
		map.put("setDadaTaskpool", 1);
		map.put("failTaskpool", 2);
		map.put("rejectUserTask", 3);
		map.put("rankLocations", 4);
		return map;
	}
	
	private static void loadDataToDB(String dealDate){
		JdbcTemplate jdbcTemplate = getJdbcTemplate();
		jdbcTemplate.execute("drop table if exists temp.dw_log_task_pool;");
		jdbcTemplate.execute("create table temp.dw_log_task_pool like bak.dw_log_task_pool;");
		jdbcTemplate.execute("alter table temp.dw_log_task_pool drop column cal_dt;");
		jdbcTemplate.execute("load data local infile '/data/dw_tmp/dw_log_task_pool_tar.sql' into table temp.dw_log_task_pool;");
		jdbcTemplate.execute("delete from bak.dw_log_task_pool where cal_dt = '"+dealDate+"';");
		jdbcTemplate.execute("insert into bak.dw_log_task_pool(cal_dt,log_type_id,city_id,error_code,task_id,transporter_id,lat,lng,distance,running_order_cnt,timeout,rank,logOrderIds,logTaskIds,reject_reason_id) select '"+dealDate+"',log_type_id,city_id,error_code,task_id,transporter_id,lat,lng,distance,running_order_cnt,timeout,rank,logOrderIds,logTaskIds,reject_reason_id from temp.dw_log_task_pool;");
		jdbcTemplate.execute("drop table if exists temp.dw_log_task_pool;");
	}
	
	public static String getTarFileName(String srcFile,String dealDate){
		String  line = null;
		Pattern pattern = Pattern.compile("(.*\\[INFO \\] -- \\[)(\\w{3,})(\\].*)");

		String tarFile  = "/data/dw_tmp/dw_log_task_pool_tar.sql";
		Map<String, Integer> cityCodeMap = getCityCode();
		Map<String, Integer> logTypeIdMap = getLogTypeId();
		StringBuilder sb = new StringBuilder();
		
		try{
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(srcFile)));
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tarFile)));
			while((line = reader.readLine())!=null){
				
				Matcher matcher = pattern.matcher(line);
				if(matcher.matches()){
					int log_type_id = logTypeIdMap.get(matcher.group(2));
					String logData = matcher.group(3).replace("] ","").trim();
					String[] strs = logData.split(",");
					sb.delete(0,sb.length());
					sb.append(log_type_id).append("\t");//log_type_id
					if(log_type_id == 1){
						sb.append(cityCodeMap.get(strs[0].trim())).append("\t");//city_id
						sb.append(-99).append("\t");//error_code
						sb.append("\\N").append("\t");//create_time
						sb.append(strs[1].trim()).append("\t");//task_id
						sb.append(strs[2].trim()).append("\t");//transporter_id
						sb.append(strs[3].trim()).append("\t");//lat
						sb.append(strs[4].trim()).append("\t");//lng
						sb.append(strs[5].trim()).append("\t");//distance
						sb.append(strs[6].trim()).append("\t");//running_order_cnt
						sb.append(strs[7].trim()).append("\t");//timeout
						try{
							sb.append(strs[8].trim()).append("\t");//rank
						}catch(Exception ex){
							sb.append("\\N").append("\t");//rank
						}
						sb.append("\\N").append("\t");//logOrderIds
						sb.append("\\N").append("\t");//logTaskIds
						sb.append("\\N");//reject_reason_id
					}else if(log_type_id == 2){//11个
						if(strs.length>=4){
							sb.append(cityCodeMap.get(strs[3].trim())).append("\t");//city_id
						}else{
							sb.append("\\N").append("\t");//city_id
						}
						sb.append(strs[0].trim()).append("\t");//error_code
						sb.append("\\N").append("\t");//create_time
						sb.append(strs[2].replace("taskId:","").trim()).append("\t");//task_id
						sb.append("\\N").append("\t");//transporter_id
						sb.append("\\N").append("\t");//lat
						sb.append("\\N").append("\t");//lng
						sb.append("\\N").append("\t");//distance
						sb.append("\\N").append("\t");//running_order_cnt
						sb.append("\\N").append("\t");//timeout
						sb.append("\\N").append("\t");//rank
						sb.append("\\N").append("\t");//logOrderIds
						sb.append("\\N").append("\t");//logTaskIds
						sb.append("\\N");//reject_reason_id
					}else if(log_type_id == 3){
						sb.append(cityCodeMap.get(strs[0].trim())).append("\t");//city_id
						sb.append(-99).append("\t");//error_code
						sb.append("\\N").append("\t");//create_time
						sb.append(strs[1].trim()).append("\t");//task_id
						sb.append(strs[2].trim()).append("\t");//transporter_id
						sb.append("\\N").append("\t");//lat
						sb.append("\\N").append("\t");//lng
						sb.append("\\N").append("\t");//distance
						sb.append("\\N").append("\t");//running_order_cnt
						sb.append("\\N").append("\t");//timeout
						sb.append("\\N").append("\t");//rank
						sb.append("\\N").append("\t");//logOrderIds
						sb.append("\\N").append("\t");//logTaskIds
						sb.append(strs[3].trim());//reason_id
					}else if(log_type_id == 4){
						sb.append(cityCodeMap.get(strs[0].trim())).append("\t");//city_id
						sb.append(-99).append("\t");//error_code
						sb.append(strs[1].trim()).append("\t");//create_time
						sb.append(strs[2].trim()).append("\t");//task_id
						sb.append(strs[3].trim()).append("\t");//transporter_id
						sb.append(strs[4].trim()).append("\t");//lat
						sb.append(strs[5].trim()).append("\t");//lng
						
						sb.append("\\N").append("\t");//distance
						sb.append("\\N").append("\t");//running_order_cnt
						sb.append("\\N").append("\t");//timeout
						try{
							sb.append(strs[6].trim()).append("\t");	//rank
						}catch(Exception ex){
							sb.append("\\N").append("\t");//rank
						}

						
						try{
							sb.append(strs[7].trim()).append("\t");//logOrderIds
						}catch(Exception ex){
							sb.append("\\N").append("\t");//logOrderIds
						}
						
						
						try{
							sb.append(strs[8].trim()).append("\t");//logTaskIds
						}catch(Exception ex){
							sb.append("\\N").append("\t");//logTaskIds
						}
						
						sb.append("\\N");//reason_id
						
					}
					
					
					writer.write(sb.toString());
					writer.newLine();
					writer.flush();
				}else{
					System.out.println(line);
				}
			}
			writer.flush();
			writer.close();
			reader.close();
			loadDataToDB(dealDate);
		}catch(Exception ex){
			ex.printStackTrace();
		}
		return tarFile;
	}
	
	
	
	
	
}
