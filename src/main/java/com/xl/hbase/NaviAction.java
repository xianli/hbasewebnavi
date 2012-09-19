package com.xl.hbase;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

public class NaviAction extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private Logger LOG = Logger.getLogger(NaviAction.class);
	protected void doGet(HttpServletRequest request, 
						HttpServletResponse response) 
					throws ServletException, IOException {
		response.setContentType("application/json;charset=UTF-8");
		PrintWriter writer = response.getWriter();
		JSONObject rtn = new JSONObject();
		
		String table=request.getParameter("table");
		String method=request.getParameter("method");
	
		try {
			if (table==null && method==null) {
				//get meta data
				String[] tables = MetaData.getTableNames();
				for (int i=0;i<tables.length;++i) {
					rtn.put(tables[i], MetaData.getRowKeyType(tables[i]));
				}
				writer.print(rtn.toString());
			} else if ("get".equals(method)) {
				String conditions=request.getParameter("conditions");
				Reader reader = new Reader(table);
				//analyze the keys;
				String[] keys = conditions.split("-");
				List<String> realKey=new ArrayList<String>();
				//make sure every key has value
				for (int i=0;i<keys.length;++i) {
					int posi = keys[i].indexOf(".")+1;
					if (posi < keys[i].length()) {
						realKey.add(keys[i].substring(posi));
					}
				}
				byte[] key = Util.string2byte(realKey, MetaData.getRowKeyType(table));
				writer.print(reader.get(key).toString());
			} else if ("scan".equals(method)) {
				int rowNum=Integer.parseInt(request.getParameter("rowNum"));
				Reader reader = new Reader(table);
				writer.print(reader.get(rowNum).toString());
			} else if ("count".equals(method)) {
				Reader reader = new Reader(table);
				writer.print(String.valueOf(reader.count()));
			} else {
				writer.print(new JSONObject().put("fail", "invalid method!").toString());
			}
		} catch (JSONException e) {
			LOG.error("json exception", e);
		}
	}
}
