package com.cjbdi.ws.services;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.rmi.RemoteException;

import javax.xml.namespace.QName;
import javax.xml.rpc.ParameterMode;

import org.apache.axis.client.Call;
import org.apache.axis.client.Service;
import org.apache.axis.encoding.XMLType;

public class WsService {
	private static final QName TNS = new QName("http://web.writstore.thunisoft.com");
	private static final QName XSD = new QName("http://www.w3.org/2001/XMLSchema");
	
	private URL     _url     = null;
	private int     _timeout = 5000;	// 5秒
	private Service service  = null;
	private Call    call     = null;
	private Object[] params  = new Object[1];
	
	public WsService(String url) {
		try {
			_url = new URL(url);
			create();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
	}
	public WsService(String url, int timeout) {
		try {
			_url = new URL(url);
			if (timeout >= 1000)
				_timeout = timeout;
			create();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
	}
	
	public boolean create() {
		if (call != null)
			return true;
		
		try {
			if (service == null)
				service = new Service();
			
			call = (Call) service.createCall();
			call.setTargetEndpointAddress(_url);
			call.setOperation(TNS, "getWritContent");
			call.addParameter("rowId", XMLType.XSD_STRING, ParameterMode.IN);
			call.setReturnType(XSD);
			call.setReturnClass(byte[].class);
			call.setTimeout(_timeout);	// 超时
			
			return true;
			
		} catch (Exception e) {
			call = null;
			e.printStackTrace();
			return false;
		}
	}
	
	public boolean isCreated() {
		return (call != null);
	}
	
	public synchronized byte[] getWritContent(String rowId) throws RemoteException {
		params[0] = rowId;
		return (byte[]) call.invoke(params);
	}
	
	public String getWritContentStr(String rowId) throws RemoteException {
		byte[] b = getWritContent(rowId);
		if (b != null)
			return new String(b, StandardCharsets.UTF_8);
		else
			return null;
	}
}
