import org.json.JSONException;

import org.json.JSONObject;


import junit.framework.Assert;



public class DBManager {

	final int  RECORD_SIZE = 50;
	private enum Status{
			SUCCESS, FAIL
	};
	final String status ="status";
	final String message ="message";
	
	
	private static SM s_smInstance = SMFactory.getInstance();


	public JSONObject store(String value, String key ) throws Exception  {
		
		Record rec;
		JSONObject jsonObj = new JSONObject();
		jsonObj.put("value",new String());
		jsonObj.put("key",new String());
		if(value == null || key == null){
			jsonObj.put(status, Status.FAIL.name());
			jsonObj.put(message, "Null Values are not allowed");
			return jsonObj;
		}
		
		try {
			byte [] byteValue = value.getBytes();
			rec = new Record(byteValue.length);
			rec.setBytes(byteValue);
			rec.setKey(key.getBytes());
			SM.OID oid = s_smInstance.store(rec);
			jsonObj.put(status, Status.SUCCESS.name());
			String key2 = ConvertToASCII(oid.toBytes());
			jsonObj.put("value",value);
			jsonObj.put("key",key2);
			jsonObj.put(message,"Record Stored");
			
	} catch (Exception e) {
			jsonObj.put(status, Status.FAIL.name());
			jsonObj.put(message, e.getMessage());
			
		}
		
		return jsonObj;
		
	}
	
	public JSONObject fetch(String key) throws Exception {
		JSONObject jsonObj = new JSONObject();
		jsonObj.put("value",new String());
		jsonObj.put("key",new String());
		if(key == null){
			jsonObj.put(status, Status.FAIL.name());
			jsonObj.put(message, "Null Values are not allowed");
			return jsonObj;
		}
		
		
		try{
        byte[][] pool = new byte[1][32] ;
		byte[] guid_bytes = key.getBytes();
		for (int i =0;i<guid_bytes.length;i++){
			pool[0][i]=guid_bytes[i];
		}
        SM.OID fetchOID = s_smInstance.getOID(pool[0]);
   
        SM.Record found = s_smInstance.fetch( fetchOID );
       if(found == null) throw new Exception();
       String value = ConvertToASCII(found.getBytes(0, 0)); 
		jsonObj.put("value",value);
		jsonObj.put("key",key);
		jsonObj.put(status, Status.SUCCESS.name());
		jsonObj.put(message,"Record found");
       
		} catch (Exception e) {
			jsonObj.put(status, Status.FAIL.name() );
			jsonObj.put(message, " Record Not Found");
			
			
		}
		
		return jsonObj;
	}
	
	JSONObject update(String value, String key ) throws Exception 
			 {
		
		
		
		Record rec;
		JSONObject jsonObj = new JSONObject();
		
		jsonObj.put("value",new String());
		jsonObj.put("key",new String());
		if(key == null || value == null){
			jsonObj.put(status, Status.FAIL.name());
			jsonObj.put(message, "Null Values are not allowed");
			return jsonObj;
		}
		
		try{
        byte[][] pool = new byte[1][32] ;
		byte[] guid_bytes = key.getBytes();
		for (int i =0;i<guid_bytes.length;i++){
			pool[0][i]=guid_bytes[i];
		}
        SM.OID UpdateOID = s_smInstance.getOID(pool[0]);
        byte [] byteValue = value.getBytes();
		rec = new Record(byteValue.length);
		rec.setBytes(byteValue);
		UpdateOID = s_smInstance.update(UpdateOID, rec);
		String key2 = ConvertToASCII(UpdateOID.toBytes());
		jsonObj.put("value",value);
		jsonObj.put("key",key2);
		jsonObj.put(message,"Record found");
		jsonObj.put(status, Status.SUCCESS.name());
       
		} catch (Exception e) {
			jsonObj.put(status, Status.FAIL.name() + " Record Not Found");
			jsonObj.put(message, "Exception:"+e.getMessage());
			
			
		}
		
		return jsonObj;
	}
	
	JSONObject delete(String key ) throws Exception 
			 {
		JSONObject jsonObj = new JSONObject();
		jsonObj.put("value",new String());
		jsonObj.put("key",new String());
		if(key == null){
			jsonObj.put(status, Status.FAIL.name());
			jsonObj.put(message ,"Null Values are not allowed");
			return jsonObj;
		}
		
	try{
		byte[][] pool = new byte[1][32] ;
		byte[] guid_bytes = key.getBytes();
		for (int i =0;i<guid_bytes.length;i++){
			pool[0][i]=guid_bytes[i];
		}
		SM.OID deleteOID = s_smInstance.getOID(pool[0]);
		s_smInstance.delete(deleteOID);
		jsonObj.put("key",key);
		jsonObj.put(status, Status.SUCCESS.name());
		jsonObj.put(message, "Record Found");
		
		} catch (Exception e) {
			jsonObj.put(status, Status.FAIL.name() + " Record Not Found");
			jsonObj.put(message, " Record Not Found");
			
			
		}
		
		return jsonObj;
		
		
		}
	
	
	
	public String ConvertToASCII(byte [] bytevalue) {
		int lastGoodChar=0;
		for(int i=0;i<bytevalue.length;i++)
        {
                int bint = new Byte(bytevalue[i]).intValue();
                if(bint == 0)
                {
                     lastGoodChar = i;
                     break;
                }
        }
		if(lastGoodChar==0){
			lastGoodChar=bytevalue.length;
		}

        String StringValue = new String(bytevalue,0,lastGoodChar);
        return StringValue;
	   
	  
	}
	
	
	

	public static void main(String[] args) throws Exception {
		DBManager dbm = new DBManager();
		JSONObject jsonobj ;
jsonobj = dbm.store("deletetestV", "deletetestK");
	  //  System.out.println(jsonobj.toString());
		jsonobj = dbm.fetch("deletetestK");
	    System.out.println(jsonobj.toString());
	jsonobj = dbm.update("deletetestV2","deletetestK");
System.out.println(jsonobj.toString());
		jsonobj = dbm.fetch("deletetestK");
	System.out.println(jsonobj.toString());
	jsonobj = dbm.delete("deletetestK");
	jsonobj = dbm.fetch("deletetestK");
	System.out.println(jsonobj.toString());
	
		
	}
	
/*
	

	@Override
	public void delete(OID oid) throws NotFoundException, CannotDeleteException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void flush() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public OID getOID(byte[] bytes) {
		// TODO Auto-generated method stub
		return null;
	}
	*/
	
}
