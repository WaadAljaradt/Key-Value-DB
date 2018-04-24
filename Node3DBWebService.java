

import java.io.BufferedReader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.Socket;
import java.net.URL;
import java.net.URLEncoder;

import javax.ws.rs.Consumes;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HTTP;
import org.json.JSONException;
import org.json.JSONObject;


 
@Path("/DbService")
public class DBWebService{
	final String status ="status";
	final String message ="message";
	private enum Operation{
		STORE, FETCH,UPDATE,DELETE
};
private enum Status{
	SUCCESS, FAIL
};
String masterHost = "http://172.30.2.219:8084/DbWebService/rest/DbService";
String host = "172.30.2.219";
String modeFile = "mode.txt";
	
private static DBManager dbm =  new DBManager();
	// for internal communication with other nodes
	@POST
	@Path("/instore")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response instore (PathContext pathContext) throws Exception{
	 JSONObject jsonObj = new JSONObject();
	 ResponseObj responseObj = new ResponseObj();
	 jsonObj = dbm.store( pathContext.getValue(),pathContext.getKey());
	 responseObj.setKey(jsonObj.getString("key"));
	 responseObj.setValue(jsonObj.getString("value"));
	 responseObj.setStatus(jsonObj.getString(status));
	 responseObj.setMessage(jsonObj.getString(message));
	 responseObj.setOperation(Operation.STORE.name());
	 return Response.status(200).entity(responseObj).build();
	 }
	// open to the world to store
	@POST
	@Path("/conf")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response conf (PathContext pathContext) throws Exception{
		 boolean frwrdToMaster = forwardToMasterWrite( pathContext.getValue(),"",masterHost+"/conf","CONF");

	 
	 return Response.status(200).entity(frwrdToMaster).build();
	}
	

	@POST
	@Path("/setmode")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response setmode  (PathContext pathContext) throws Exception{
	 String value = pathContext.getValue();
			File file = new File(modeFile);
			try {	
						if (!file.exists()) {
							file.createNewFile();
							
						}

						FileWriter fw;
					
						fw = new FileWriter(file.getAbsoluteFile());
						
						BufferedWriter bw = new BufferedWriter(fw);
						bw.write(String.valueOf(value));
						bw.close();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
					
						}

	 return Response.status(200).entity(true).build();
	 }
	public int getMode(){
		File file = new File(modeFile);
		String conf="1";
		try{
		if(file.exists()){
		
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String currentLine = reader.readLine();
		if(currentLine != null || currentLine.length()!= 0) 
			conf =  currentLine.trim();
		return Integer.valueOf(conf);

	}else{
		return 1;
	}
	}catch (Exception ex){
		return 1;
	}
	}
	
	@POST
	@Path("/store")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response store (PathContext pathContext) throws Exception{
		JSONObject jsonObj = new JSONObject();
		ResponseObj responseObj = new ResponseObj();
	
	 String writeValue = pathContext.getValue();
	 String writeKey = pathContext.getKey();
	if(Synchronize()){
	boolean frwrdToMaster = forwardToMasterWrite( writeValue,writeKey,masterHost+"/store",Operation.STORE.name());

	if (frwrdToMaster){// CP check internally and return decline other wise
		   jsonObj = dbm.fetch(pathContext.getKey());
		   String status = jsonObj.get("status")!=null ? String.valueOf(jsonObj.get("status")) : null;
		   System.out.println("status: "+status+ jsonObj.get("message"));
		   if(status !=null && status.equalsIgnoreCase(Status.SUCCESS.name())){
		   	responseObj.setKey(jsonObj.getString("key"));
			 responseObj.setValue(jsonObj.getString("value"));
			 responseObj.setStatus(jsonObj.getString("status"));
			 responseObj.setMessage("Record Stored");
			 responseObj.setOperation(Operation.STORE.name());
		   }else{
			   responseObj.setMessage("Record is Not stored failed to write to node");
			   responseObj.setOperation(Operation.STORE.name());
			   
		   }
		   
	}else {
		 responseObj.setMessage("Record is Not stored,Record is Not stored Service is Not Available");
		 responseObj.setOperation(Operation.STORE.name());
		
		
	}
	}else {
		 responseObj.setMessage("Record is Not stored,Record is Not stored Service is Not Available");
		 responseObj.setOperation(Operation.STORE.name());
	}
		
	 String myAddr = String.valueOf(InetAddress.getLocalHost().getHostAddress());
	 responseObj.setIpAddress(myAddr);
		return Response.status(200).entity(responseObj).build();
		
		
		 }
	
	
	@POST
	@Path("/delete")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response delete (PathContext pathContext) throws Exception{
		
		JSONObject jsonObj = new JSONObject();
		ResponseObj responseObj = new ResponseObj();
	if(Synchronize()){
	
	 String writeValue = pathContext.getValue();
	 String writeKey = pathContext.getKey();
	
	 boolean frwrdToMaster = forwardToMasterWrite( writeValue,writeKey,masterHost+"/delete",Operation.DELETE.name());

		if (frwrdToMaster){// CP check internally and return decline other wise
			   jsonObj = dbm.fetch(pathContext.getKey());
			   String status = jsonObj.get("status")!=null ? String.valueOf(jsonObj.get("status")) : null;
			   System.out.println("status: "+status+ jsonObj.get("message"));
			   if(status !=null && status.equalsIgnoreCase(Status.FAIL.name())){
			   	responseObj.setKey(jsonObj.getString("key"));
				 responseObj.setValue(jsonObj.getString("value"));
				 responseObj.setStatus(Status.SUCCESS.name());
				 responseObj.setMessage("Record Deleted");
				 responseObj.setOperation(Operation.DELETE.name());
			   }else{
				   responseObj.setMessage("Record is Not Deleted failed to delete internally");
				   responseObj.setOperation(Operation.DELETE.name());
				   
			   }
	}else {
		 responseObj.setMessage("Record is Not Deleted, Service is Not Available");
		 responseObj.setOperation(Operation.DELETE.name());
		
		
	}
	}else {
			 responseObj.setMessage("Record is Not Deleted, Service is Not Available");
			 responseObj.setOperation(Operation.DELETE.name());
		}
		
	 String myAddr = String.valueOf(InetAddress.getLocalHost().getHostAddress());
	 responseObj.setIpAddress(myAddr);	
	return Response.status(200).entity(responseObj).build();
	 }
	

	@POST
	@Path("/update")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response update (PathContext pathContext) throws Exception{
		
		JSONObject jsonObj = new JSONObject();
		ResponseObj responseObj = new ResponseObj();
		if(Synchronize()){
	 
	 String writeValue = pathContext.getValue();
	 String writeKey = pathContext.getKey();
	 
	 boolean frwrdToMaster = forwardToMasterWrite( writeValue,writeKey,masterHost+"/update",Operation.UPDATE.name());

		if (frwrdToMaster){// CP check internally and return decline other wise
			   jsonObj = dbm.fetch(pathContext.getKey());
			   String status = jsonObj.get("status")!=null ? String.valueOf(jsonObj.get("status")) : null;
			   System.out.println("status: "+status+ jsonObj.get("message"));
			   if(status !=null && status.equalsIgnoreCase(Status.SUCCESS.name()) && writeValue.equalsIgnoreCase(jsonObj.getString("value")) ){
			   	responseObj.setKey(jsonObj.getString("key"));
				 responseObj.setValue(jsonObj.getString("value"));
				 responseObj.setStatus(jsonObj.getString("status"));
				 responseObj.setMessage("Record UPDATEd");
				 responseObj.setOperation(Operation.UPDATE.name());
			   }else{
				   responseObj.setMessage("Record is Not UPDATED failed to update internally");
				   responseObj.setOperation(Operation.UPDATE.name());
				   
			   }
			   
		}else {
			 responseObj.setMessage("Record is Not Updated, Service is Not Available");
			 responseObj.setOperation(Operation.UPDATE.name());
			
			
		}
	}else {
		 responseObj.setMessage("Record is Not Updated, Service is Not Available");
		 responseObj.setOperation(Operation.UPDATE.name());
		
		
	}
	 
		 String myAddr = String.valueOf(InetAddress.getLocalHost().getHostAddress());
		 responseObj.setIpAddress(myAddr);
	return Response.status(200).entity(responseObj).build();
	 }
	
	
	// Forwards Writes To Master
	 public boolean forwardToMasterWrite(String writeValue, String writeKey,String url,String operation){
		 String result=new String();
		 try{
			 HttpPost request = new HttpPost(url);
		       JSONObject json = new JSONObject();
		
		        json.put("value", writeValue);
		        json.put("key", writeKey);
		        StringEntity entity = new StringEntity(json.toString());
	          entity.setContentType("application/json");
	          entity.setContentEncoding(new BasicHeader(HTTP.CONTENT_TYPE,"application/json"));
	          request.setHeader("Accept", "application/json");
	          request.setEntity(entity); 
	          HttpResponse response =null;
	          DefaultHttpClient httpClient = new DefaultHttpClient();
	          response = httpClient.execute(request); 
	          InputStream in = response.getEntity().getContent();
	          result = convertStreamToString(in);
	          System.out.print(result);
	         int code = response.getStatusLine().getStatusCode();
	         if (code != 200 ){
	        	 System.out.print("Failed to write to master");
	        	 return false;
	         }
	        
		
	 }
        catch(Exception se)
        {
	
      	  System.out.print("failed to write to Node 2");
      	return false;
        }
	return true;
	}
	
	 
	 public boolean isSynch(){
		 String result=new String();
		 boolean isSynch= false;
		 try{
			 HttpPost request = new HttpPost(masterHost+"/isSynch");
		       JSONObject json = new JSONObject();
		        json.put("key", "node3");
		        StringEntity entity = new StringEntity(json.toString());
	          entity.setContentType("application/json");
	          entity.setContentEncoding(new BasicHeader(HTTP.CONTENT_TYPE,"application/json"));
	          request.setHeader("Accept", "application/json");
	          request.setEntity(entity); 
	          HttpResponse response =null;
	          DefaultHttpClient httpClient = new DefaultHttpClient();
	          response = httpClient.execute(request); 
	          InputStream in = response.getEntity().getContent();
	          result = convertStreamToString(in);
	          System.out.println(result);
	          result = result.replace("\"", "");
	         int code = response.getStatusLine().getStatusCode();
	         System.out.print(result);
	         if (code != 200 ){
	        	 System.out.print("Failed to write to master");
	        	 return false;
	         }
	         if(result.trim().equalsIgnoreCase("true")) {
	        	  isSynch = true;
	        	  System.out.println("isSynch should be true");
	          }     
		
	 }
        catch(Exception se)
        {
      	  System.out.print(se.getMessage());
      	return false;
        }
		
	return isSynch;
	}
	 
	 	@POST
		@Path("/synchWith")
		@Consumes(MediaType.APPLICATION_JSON)
		@Produces(MediaType.APPLICATION_JSON)
		public Response synchWith(PathContext pathContext) throws Exception{
		 String result=new String();
		 
		  boolean isSynch= false;
		  String str = "";
		
		 try{
			 HttpPost request = new HttpPost(masterHost+"/synch");
		       JSONObject json = new JSONObject();
		        json.put("key", "node3");
		        StringEntity entity = new StringEntity(json.toString());
	          entity.setContentType("application/json");
	          entity.setContentEncoding(new BasicHeader(HTTP.CONTENT_TYPE,"application/json"));
	          request.setHeader("Accept", "application/json");
	          request.setEntity(entity); 
	          HttpResponse response =null;
	          DefaultHttpClient httpClient = new DefaultHttpClient();
	          response = httpClient.execute(request); 
	          InputStream in = response.getEntity().getContent();
	          result = convertStreamToString(in);
	          result = result.replace("\"", "");
	          str = str+"result:"+result;
	          
	          if(result.trim().equalsIgnoreCase("true")) {
	        	  isSynch = true;
	        	  str = str+"isSynch should be true";
	          }
	         
	          
	         int code = response.getStatusLine().getStatusCode();
	         if (code != 200 ){
	        	 System.out.print("Failed to synch to master");
	        	 isSynch = false;
	         }
	        	 
	         
	 }
        catch(Exception se)
        {
        	 System.out.print("Exception");
        	 se.printStackTrace();
        	isSynch = false;
      	
        }
		
	
	return Response.status(200).entity(str).build();
		 
	 }
	 
	 
	 public boolean synchWithMaster() {
		 String result=new String();
		 
		  boolean isSynch= false;
		  String str = "";
		
		 try{
			 HttpPost request = new HttpPost(masterHost+"/synch");
		       JSONObject json = new JSONObject();
		        json.put("key", "node3");
		        StringEntity entity = new StringEntity(json.toString());
	          entity.setContentType("application/json");
	          entity.setContentEncoding(new BasicHeader(HTTP.CONTENT_TYPE,"application/json"));
	          request.setHeader("Accept", "application/json");
	          request.setEntity(entity); 
	          HttpResponse response =null;
	          DefaultHttpClient httpClient = new DefaultHttpClient();
	          response = httpClient.execute(request); 
	          InputStream in = response.getEntity().getContent();
	          result = convertStreamToString(in);
	          result = result.replace("\"", "");
	          str = str+"result:"+result;
	          
	          if(result.trim().equalsIgnoreCase("true")) {
	        	  isSynch = true;
	        	  str = str+"isSynch should be true";
	          }
	         
	          
	         int code = response.getStatusLine().getStatusCode();
	         if (code != 200 ){
	        	 System.out.print("Failed to synch to master");
	        	 isSynch = false;
	         }
	        	 
	         
	 }
       catch(Exception se)
       {
       	 System.out.print("Exception");
       	 se.printStackTrace();
       	isSynch = false;
     	
       }
		
	return isSynch;
	}
	@POST
	@Path("/test3")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response test (PathContext pathContext) throws Exception{
		int getMode = getMode();
		String str = String.valueOf(getMode);
		if(getMode==2) str =str+"PA";
		else str =str+"CP";
	return Response.status(200).entity(getMode).build();
	
	
	
	 }
	private boolean Synchronize(){
		Boolean isMasterPartition= null,isSynch,sychOpera = null;
		isMasterPartition = isMasterPartition();
		if(!isMasterPartition){
			 isSynch = isSynch();
			System.out.println(isSynch);
			if(isSynch) return true;
			else{
				sychOpera= synchWithMaster();
				if(sychOpera) return true;
		}
		}else{
			return false;
		}
		return false;
	}
	@POST
	@Path("/fetch")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response fetch (PathContext pathContext) throws Exception{
		 ResponseObj responseObj = new ResponseObj();
	 if(Synchronize()){
	
	 JSONObject  jsonObj = dbm.fetch(pathContext.getKey());
	 System.out.println(jsonObj.toString());
	 responseObj.setKey(jsonObj.getString("key"));
	 responseObj.setValue(jsonObj.getString("value"));
	 responseObj.setStatus(jsonObj.getString(status));
	 responseObj.setMessage(jsonObj.getString(message));
	 responseObj.setOperation(Operation.FETCH.name());
	}else{
			 if(getMode()==2){
				 JSONObject  jsonObj = dbm.fetch(pathContext.getKey());
				 System.out.println(jsonObj.toString());
				 responseObj.setKey(jsonObj.getString("key"));
				 responseObj.setValue(jsonObj.getString("value"));
				 responseObj.setStatus(jsonObj.getString(status));
				 responseObj.setOperation(Operation.FETCH.name());
				 responseObj.setMessage("data could be outdated, in partition mode");
				 
			 }else{
			 responseObj.setMessage("Service unavailbe");
	}
	}
	 String myAddr = String.valueOf(InetAddress.getLocalHost().getHostAddress());
	 responseObj.setIpAddress(myAddr);
	return Response.status(200).entity(responseObj).build();
	 }
	

	
	@POST
	@Path("/inupdate")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response inupdate (PathContext pathContext) throws Exception{

	 ResponseObj responseObj = new ResponseObj();
	 JSONObject  jsonObj = dbm.update(pathContext.getValue(),pathContext.getKey());
	 responseObj.setKey(jsonObj.getString("key"));
	 responseObj.setValue(jsonObj.getString("value"));
	 responseObj.setStatus(jsonObj.getString(status));
	 responseObj.setMessage(jsonObj.getString(message));
	 responseObj.setOperation(Operation.UPDATE.name());
	return Response.status(200).entity(responseObj).build();
	 }
	
	
	@POST
	@Path("/indelete")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response indelete (PathContext pathContext) throws Exception{
	 ResponseObj responseObj = new ResponseObj();
	 JSONObject  jsonObj = dbm.delete(pathContext.getKey());
	 responseObj.setKey(jsonObj.getString("key"));
	responseObj.setValue(jsonObj.getString("value"));
	 responseObj.setStatus(jsonObj.getString(status));
	 responseObj.setOperation(Operation.DELETE.name());
	 responseObj.setMessage(jsonObj.getString(message));
	return Response.status(200).entity(responseObj).build();
	 }
	
	
	private static String convertStreamToString(InputStream is) {

	    BufferedReader reader = new BufferedReader(new InputStreamReader(is));
	    StringBuilder sb = new StringBuilder();

	    String line = null;
	    try {
	        while ((line = reader.readLine()) != null) {
	            sb.append(line + "\n");
	        }
	    } catch (IOException e) {
	        e.printStackTrace();
	    } finally {
	        try {
	            is.close();
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	    }
	    return sb.toString();
	}

public boolean	isMasterPartition(){
		return !isPortReachable(host, 8084);
	}
	
	boolean isPortReachable(String host, int port) {
		
		 try
         {
		InetSocketAddress sAdress = new InetSocketAddress(host, port);
          Socket ServerSok = new Socket();
          ServerSok.connect(sAdress,1000);
          System.out.println("Port in use: " + port );
          ServerSok.close();
          return true;
         }
         catch (Exception e)
         {
             e.printStackTrace(); 
         }
		 return false;
	}
	
	@GET
	@Path("/ping")
	public Response ping () throws Exception{
		 String myAddr = String.valueOf(InetAddress.getLocalHost().getHostAddress());
		 
	return Response.status(200).entity(myAddr).build();
	 }
	
}


