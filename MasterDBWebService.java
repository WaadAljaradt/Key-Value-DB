

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.servlet.ServletContext;
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
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.protocol.HTTP;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;

 
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

String node2Host = "http://172.30.1.92:8082/DbWebService/rest/DbService";
String node3Host = "http://172.30.0.242:8083/DbWebService/rest/DbService";
String host2 = "172.30.1.92";
String host3 = "172.30.0.242";
String node2File = "node2fail.txt";
String node3File = "node3fail.txt";
String modeFile = "mode.txt";




private static DBManager dbm =  new DBManager();


@POST
@Path("/conf")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public Response conf (PathContext pathContext) throws Exception{
// Master write operation it writes then duplicate to Nodes 
	String myAddr = String.valueOf(InetAddress.getLocalHost().getHostAddress());
	ResponseObj responseObj = new ResponseObj();
if(pathContext.getValue()==null){
	responseObj.setStatus("Failed ");
	// CP --> Consistent , PA ---> Available 
	 responseObj.setMessage("value is not accepted. CP is defaul mode, send Value =1 CP,value=2 PA");
}else{
	int Smode = Integer.valueOf(pathContext.getValue());
	String modeStr="";
	if(Smode == 2) modeStr ="PA";
	else modeStr ="CP";
	if(Smode == 2 || Smode == 1){
		boolean setMode = setMode(Smode);
		responseObj.setMessage("Conf set to mode"+modeStr+"setMode"+setMode);
		 
	}
	else{
		responseObj.setStatus("Failed ");
		responseObj.setMessage("value is not accepted. CP is defaul mode, send Value =1 CP,value=2 PA");
	}
	
}
 responseObj.setOperation("Conf");
 
 return Response.status(200).entity(responseObj).build();
}

private boolean setMode(Integer Smode){
	File file = new File(modeFile);
	try {	
				if (!file.exists()) {
					file.createNewFile();
					
				}

				FileWriter fw;
			
				fw = new FileWriter(file.getAbsoluteFile());
				
				BufferedWriter bw = new BufferedWriter(fw);
				bw.write(String.valueOf(Smode));
				bw.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return false;
				}

	
	  duplicate ( String.valueOf(Smode),  "",node2Host+"/setmode","CONF","dum.txt");
	  duplicate ( String.valueOf(Smode),  "",node3Host+"/setmode","CONF","dum.txt");
	return true;
}
	@POST
	@Path("/store")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response store (PathContext pathContext) throws Exception{
	// Master write operation it writes then duplicate to Nodes 
		String myAddr = String.valueOf(InetAddress.getLocalHost().getHostAddress());
		
	 JSONObject jsonObj = new JSONObject();
	 ResponseObj responseObj = new ResponseObj();
	 String writeValue = pathContext.getValue();
	 String writeKey = pathContext.getKey();
	 jsonObj = dbm.store( writeValue,writeKey );
	 responseObj.setKey(jsonObj.getString("key"));
	 responseObj.setValue(jsonObj.getString("value"));
	 responseObj.setStatus(jsonObj.getString(status));
	 responseObj.setMessage(jsonObj.getString(message));
	 responseObj.setOperation(Operation.STORE.name());
	
	 
	 Boolean node2 = false;
	 Boolean node3 = false;
	 // duplicates to Slave node 2
	 //localhost
	 if(jsonObj.getString(status).equalsIgnoreCase(Status.SUCCESS.name())){
		 if(isPortReachable(host2,8082)){
			 node2 = duplicate ( writeValue,  writeKey,node2Host+"/instore",Operation.STORE.name(),node2File);
	
	 }else{
		 HandleFail(writeKey,writeValue,Operation.STORE.name(),node2File);		 
	 }
		 if(isPortReachable(host3,8083)){
			 node3 = duplicate ( writeValue,  writeKey,node3Host+"/instore",Operation.STORE.name(),node3File);
	
	 }else{
		 HandleFail(writeKey,writeValue,Operation.STORE.name(),node3File);		 
	 }
		
	 }
	 responseObj.setMessage("Node2 status :"+node2+"Node3 status :"+node3);
	 responseObj.setIpAddress(myAddr);
		return Response.status(200).entity(responseObj).build();
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
	@Path("/test")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response test (PathContext pathContext) throws Exception{
			int getMode = getMode();
			String str = String.valueOf(getMode);
			if(getMode==2) str =str+"PA";
			else str =str+"CP";
		return Response.status(200).entity(getMode).build();
	 }
	
	@POST
	@Path("/deleteFile")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response deleteFile (PathContext pathContext) throws Exception{
	String MasterAddr = String.valueOf(InetAddress.getLocalHost().getHostAddress());
	File file = new File(node2File);
	String str="";
	FileWriter fileOut = new FileWriter(file);
	fileOut.write("");
	fileOut.close();
	File file3 = new File(node3File);
	
	FileWriter fileOut3 = new FileWriter(file3);
	fileOut3.write("");
	fileOut3.close();
	
	str=str+MasterAddr;
	

		return Response.status(200).entity(str).build();
	}
	
	
	@POST
	@Path("/isSynch")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response isSynch (PathContext pathContext) throws Exception{

	 ResponseObj responseObj = new ResponseObj();
	 String nodeName = pathContext.getKey();
	 boolean isSynch = CheckSynch(nodeName) ;
	return Response.status(200).entity(isSynch).build();
	 }
	
	
	
	public boolean CheckSynch (String nodeName) throws Exception{

	 boolean isSynch = false;
	
	 if (nodeName.equalsIgnoreCase("node2")){
		 
			
			File file = new File(node2File);
			
		 if (file.length() == 0) {
			 isSynch = true;
			} else{
				 isSynch = false;
			}
	 }else{
		 if (nodeName.equalsIgnoreCase("node3")){
			 
				File file = new File(node3File);
				
			 if (file.length() == 0) {
				 isSynch = true;
				} else{
					 isSynch = false;
				}
		 
	 }
	 }
	
	return isSynch;
	 }
	
	@POST
	@Path("/fetch")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response fetch (PathContext pathContext) throws Exception{

	 ResponseObj responseObj = new ResponseObj();
	 JSONObject  jsonObj = dbm.fetch(pathContext.getKey());
	 System.out.println(jsonObj.toString());
	 responseObj.setKey(jsonObj.getString("key"));
	 responseObj.setValue(jsonObj.getString("value"));
	 responseObj.setStatus(jsonObj.getString(status));
	 responseObj.setMessage(jsonObj.getString(message));
	 responseObj.setOperation(Operation.FETCH.name());
	 String myAddr = String.valueOf(InetAddress.getLocalHost().getHostAddress());
	 responseObj.setIpAddress(myAddr);
	return Response.status(200).entity(responseObj).build();
	 }
	
	@POST
	@Path("/update")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response update (PathContext pathContext) throws Exception{
		String writeValue = pathContext.getValue();
		String writeKey = pathContext.getKey();
	 ResponseObj responseObj = new ResponseObj();
	 JSONObject  jsonObj = dbm.update(pathContext.getValue(),pathContext.getKey());
	 responseObj.setKey(jsonObj.getString("key"));
	 responseObj.setValue(jsonObj.getString("value"));
	 responseObj.setStatus(jsonObj.getString(status));
	 responseObj.setMessage(jsonObj.getString(message));
	 responseObj.setOperation(Operation.UPDATE.name());
	 boolean node2=false,node3=false;
	 if(jsonObj.getString(status).equalsIgnoreCase(Status.SUCCESS.name())){
		 if(isPortReachable(host2,8082)){ // duplicate to slave nodes
	 node2 = duplicate ( writeValue,  writeKey,node2Host+"/inupdate",Operation.UPDATE.name(),node2File);
		 }
		 else{
			 HandleFail(writeKey,writeValue,Operation.UPDATE.name(),node2File);		 
		 }
		 if(isPortReachable(host3,8083)){
			 node3 = duplicate ( writeValue,  writeKey,node3Host+"/inupdate",Operation.UPDATE.name(),node3File);
	
	 }else{
		 HandleFail(writeKey,writeValue,Operation.UPDATE.name(),node3File);		 
	 }
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
		String writeValue = pathContext.getValue();
		String writeKey = pathContext.getKey();
		
	 ResponseObj responseObj = new ResponseObj();
	 JSONObject  jsonObj = dbm.delete(pathContext.getKey());
	 responseObj.setKey(jsonObj.getString("key"));
	responseObj.setValue(jsonObj.getString("value"));
	 responseObj.setStatus(jsonObj.getString("status"));
	 responseObj.setOperation(Operation.DELETE.name());
	responseObj.setMessage(jsonObj.getString(message));
	boolean node2=false,node3=false;
	 if(jsonObj.getString(status).equalsIgnoreCase(Status.SUCCESS.name())){
		 if(isPortReachable(host2,8082)){
	 node2 = duplicate ( writeValue,  writeKey,node2Host+"/indelete",Operation.DELETE.name(),node2File);
	 }else{
		 HandleFail(writeKey,writeValue,Operation.DELETE.name(),node2File);		 
	 }
		 if(isPortReachable(host3,8083)){
			 node3 = duplicate ( writeValue,  writeKey,node3Host+"/indelete",Operation.DELETE.name(),node3File);
	
	 }else{
		 HandleFail(writeKey,writeValue,Operation.DELETE.name(),node3File);		 
	 }
		 
	 }
	 String myAddr = String.valueOf(InetAddress.getLocalHost().getHostAddress());
	 responseObj.setIpAddress(myAddr);
	return Response.status(200).entity(responseObj).build();
	 }
	
	@POST
	@Path("/synch")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response synch (PathContext pathContext) throws Exception{

	// ResponseObj responseObj = new ResponseObj();
	System.out.println("In Synch");
	String NodeName = pathContext.getKey();
	Boolean node2= false;
	if(NodeName.equals("node2")  && !CheckSynch(NodeName)){
		 node2 = SynchNode2();
		
	}
	if(NodeName.equals("node3")  && !CheckSynch(NodeName)){
		 node2 = SynchNode3();
		
	}
	 
	return Response.status(200).entity(node2).build();
	 }
	// Duplicate write operation to master 
	public Boolean duplicate (String writeValue, String writeKey, String url,String operation,String fileName){
		 StringBuilder sb=new StringBuilder();
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
	         int code = response.getStatusLine().getStatusCode();
	         InputStream in = response.getEntity().getContent();
         	BufferedReader reader = new BufferedReader(new InputStreamReader(in));
         	String line = null;
            
         	while((line = reader.readLine()) != null){
         		sb.append(line);

         	}
	         System.out.println(sb);
	         if (code != 200 ){
	        	 HandleFail(writeKey,writeValue,operation,fileName);
	        	return false;
		 }	
		 
		}
		
        catch(Exception se)
        {
        	 HandleFail(writeKey,writeValue,operation,fileName);
      	  System.out.print("failed to write to Node 2");
      	 return false;
      
        }
		return true;
		
	}

	public void HandleFail(String key, String value,String operation,String fileName){
		
		File file = new File(fileName);
		
try{
			
			if (!file.exists()) {
				file.createNewFile();
				
			}

			FileWriter fw = new FileWriter(file.getAbsoluteFile(),true);
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(key+","+value+","+operation+"\n");
			bw.close();
		
	}catch(Exception ex){
		
	}

	}
	
	

	
	public boolean SynchNode2(){
		String currentLine,writeKey,writeValue,operation;
		String str ="";
		Boolean node2 = false;
		try {
			File file = new File(node2File);
			BufferedReader reader = new BufferedReader(new FileReader(file));
			while((currentLine = reader.readLine()) != null && currentLine.length() != 0) {
				String[] var = currentLine.split(",");
				writeKey = var[0];
				writeValue = var[1];
				operation = var[2];
				str=str+writeKey+writeValue+operation;
				System.out.println("Read File : "+writeKey+writeValue+operation);
				if(operation.equals(Operation.STORE.name())){
					 node2 = duplicate( writeValue,  writeKey,node2Host+"/instore",Operation.STORE.name(),node2File);
				
				}else if(operation.equals(Operation.UPDATE.name())){
					 node2 = duplicate( writeValue,  writeKey,node2Host+"/inupdate",Operation.UPDATE.name(),node2File);

				}else if(operation.equals(Operation.DELETE.name())){
					 node2 = duplicate( writeValue,  writeKey,node2Host+"/indelete",Operation.DELETE.name(),node2File);
				}
				
				
				if(node2){
						removeLineFromFile(node2File,currentLine);
					}
					
				}
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//return Response.status(200).entity(node2).build();
		return node2;
	}
	
	
	public boolean SynchNode3(){
		String currentLine,writeKey,writeValue,operation;
		String str ="";
		Boolean node3 = false;
		try {
			File file = new File(node3File);
			BufferedReader reader = new BufferedReader(new FileReader(file));
			while((currentLine = reader.readLine()) != null && currentLine.length() != 0) {
				String[] var = currentLine.split(",");
				writeKey = var[0];
				writeValue = var[1];
				operation = var[2];
				str=str+writeKey+writeValue+operation;
				System.out.println("Read File : "+writeKey+writeValue+operation);
				if(operation.equals(Operation.STORE.name())){
					node3 = duplicate( writeValue,  writeKey,node3Host+"/instore",Operation.STORE.name(),node3File);
				
				}else if(operation.equals(Operation.UPDATE.name())){
					node3 = duplicate( writeValue,  writeKey,node3Host+"/inupdate",Operation.UPDATE.name(),node3File);

				}else if(operation.equals(Operation.DELETE.name())){
					node3 = duplicate( writeValue,  writeKey,node3Host+"/indelete",Operation.DELETE.name(),node3File);
				}
				
				
				if(node3){
						removeLineFromFile(node3File,currentLine);
					}
					
				}
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//return Response.status(200).entity(node2).build();
		return node3;
	}
	
	
	
	boolean isPortReachable(String host, int port) {
		
		 try
        {
		InetSocketAddress sAdress = new InetSocketAddress(host, port);
         Socket ServerSok = new Socket();
         ServerSok.connect(sAdress,500);
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
	
	
	public void removeLineFromFile(String file, String lineToRemove) {
		 
	    try {
	 
	      File inFile = new File(file);
	      
	      if (!inFile.isFile()) {
	        System.out.println("Parameter is not an existing file");
	        return;
	      }
	       
	      //Construct the new file that will later be renamed to the original filename. 
	      File tempFile = new File(inFile.getAbsolutePath() + ".tmp");
	      
	      BufferedReader br = new BufferedReader(new FileReader(file));
	      PrintWriter pw = new PrintWriter(new FileWriter(tempFile));
	      
	      String line = null;
	 
	      //Read from the original file and write to the new 
	      //unless content matches data to be removed.
	      while ((line = br.readLine()) != null) {
	        
	        if (!line.trim().equals(lineToRemove)) {
	 
	          pw.println(line);
	          pw.flush();
	        }
	      }
	      pw.close();
	      br.close();
	      
	      //Delete the original file
	      if (!inFile.delete()) {
	        System.out.println("Could not delete file");
	        return;
	      } 
	      
	      //Rename the new file to the filename the original file had.
	      if (!tempFile.renameTo(inFile))
	        System.out.println("Could not rename file");
	      
	    }
	    catch (FileNotFoundException ex) {
	      ex.printStackTrace();
	    }
	    catch (IOException ex) {
	      ex.printStackTrace();
	    }
	  }
	@GET
	@Path("/ping")
	public Response ping () throws Exception{
		 String myAddr = String.valueOf(InetAddress.getLocalHost().getHostAddress());
		 
	return Response.status(200).entity(myAddr).build();
	 }
	
	
}


