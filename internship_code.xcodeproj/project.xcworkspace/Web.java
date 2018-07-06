package com.dinfo.bigdata.web;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.log4j.Logger;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.alibaba.fastjson.JSON;
import com.dinfo.bigdata.hbase.HBaseApi;
import com.dinfo.bigdata.hdfs.FileReadFromHdfs;
import com.dinfo.common.model.Response;

@RestController
@RequestMapping("/hbase")
public class HbaseWeb {
	private final Logger logger = Logger.getLogger(getClass());
	private HBaseApi hbasec = null;
	
	@RequestMapping(value = "/createtable", method = RequestMethod.GET)
	public Response<Boolean> createTable(@RequestParam String tableNmae, @RequestParam String[] cols) {
		hbasec = new HBaseApi();
		try {
			hbasec.createTable(tableNmae, cols);
			return Response.ok(true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return Response.ok(false);
	}
	
	@RequestMapping(value = "/deletetable", method = RequestMethod.GET)
	public Response<Boolean> deleteTable(@RequestParam String tableNmae) {
		hbasec = new HBaseApi();
		try {
			hbasec.deleteTable(tableNmae);
			return Response.ok(true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return Response.ok(false);
	}
	
	@RequestMapping(value = "/listtables")
	public Response<List<String>> listTables() {
		hbasec = new HBaseApi();
		List<String> retList = new ArrayList<String>(10);
		try {
			HTableDescriptor[] htables = hbasec.listTables();
			for (HTableDescriptor hTableDescriptor : htables) {
				retList.add(hTableDescriptor.getNameAsString());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return Response.ok(retList);
	}
	
	@RequestMapping(value = "/istable")
	public Response<Boolean> isTable(String tableName) {
		hbasec = new HBaseApi();
		Boolean bool = false;
		try {
			bool = hbasec.isTable(tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return Response.ok(bool);
	}
	
	@RequestMapping(value = "/insterrow", method = RequestMethod.GET)
	public Response<Boolean> insterRow(String tableName, String rowkey, String colFamily, String col, String val) {
		hbasec = new HBaseApi();
		boolean b = hbasec.insterRow(tableName, rowkey, colFamily, col, val);
		return Response.ok(b);
	}
	
	@RequestMapping(value = "/insterrowmap")
	public Response<Boolean> insterRowMap(@RequestBody Response response) {
		hbasec = new HBaseApi();
		String tableName, rowkey, colFamily, col, val;
		Map<String, String> inmap = (Map<String, String>) response.getData();
		tableName = inmap.get("tablename");
		rowkey = inmap.get("rowkey");
		colFamily = inmap.get("colfamily");
		col = inmap.get("col");
		val = inmap.get("val");
		
		boolean b = hbasec.insterRow(tableName, rowkey, colFamily, col, val);
		return Response.ok(b);
	}
	
	@RequestMapping(value = "/insterrowmaps")
	public Response<Boolean> insterRowMaps(String tableName, @RequestBody Response response) {
		hbasec = new HBaseApi();
		List<Map<String, String>> inmap = (List<Map<String, String>>) response.getData();
		
		try {
			hbasec.insterRows(tableName, inmap);
			return Response.ok(true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return Response.ok(false);
	}
	
	@RequestMapping(value = "/insterrowmapsret")
	public Response<List<Map<String, String>>> insterRowMapsRet(@RequestBody Response response) {
		hbasec = new HBaseApi();
		List<Map<String, String>> inmap = (List<Map<String, String>>) response.getData();
		
		List<Map<String, String>> retList = null;
		try {
			retList = hbasec.insterrowmapsret(inmap);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return Response.ok(retList);
	}
	
	@RequestMapping(value = "/delerow", method = RequestMethod.GET)
	public Response<Boolean> deleRow(String tableName, String rowkey, String colFamily, String col) {
		hbasec = new HBaseApi();
		try {
			hbasec.deleRow(tableName, rowkey, colFamily, col);
			return Response.ok(true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return Response.ok(false);
	}
	
	@RequestMapping(value = "/delerex", method = RequestMethod.GET)
	public Response<Boolean> deleRowRex(String tableName, String rowkey) {
		hbasec = new HBaseApi();
		return Response.ok(hbasec.delRegexKey(tableName, rowkey));
	}
	
	@RequestMapping(value = "/getdata")
	public Response<List<List<String[]>>> getData(String tableName, String rowkey, String colFamily, String col) {
		hbasec = new HBaseApi();
		List<List<String[]>> retList = new ArrayList<List<String[]>>();
		try {
			Result r = hbasec.getData(tableName, rowkey, colFamily, col);
			Cell[] cells = r.rawCells();
			String[] tmpStrs = null;
			List<String[]> tmpList = new ArrayList<String[]>(10);
			retList.add(tmpList);
			for (Cell cell : cells) {
				tmpStrs = new String[5];
				tmpList.add(tmpStrs);
				tmpStrs[0] = new String(CellUtil.cloneRow(cell));
				tmpStrs[1] = String.valueOf(cell.getTimestamp());
				tmpStrs[2] = new String(CellUtil.cloneFamily(cell));
				tmpStrs[3] = new String(CellUtil.cloneQualifier(cell));
				tmpStrs[4] = new String(CellUtil.cloneValue(cell));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return Response.ok(retList);
	}
	
	@RequestMapping(value = "/getdataregex")
	public Response<List<List<String[]>>> getDataRegex(	String tableName, String rowkey, Integer currentPage,
														Integer pageSize, Boolean reversed) {
		hbasec = new HBaseApi();
		List<Result> rs = null;
		List<List<String[]>> retList = new ArrayList<List<String[]>>();
		String[] tmpStrs = null;
		List<String[]> tmpList = null;
		try {
			rs = hbasec.getDataRegex(tableName, rowkey, currentPage, pageSize, reversed);
			for (Result result : rs) {
				Cell[] cells = result.rawCells();
				tmpList = new ArrayList<String[]>(10);
				retList.add(tmpList);
				for (Cell cell : cells) {
					tmpStrs = new String[5];
					tmpList.add(tmpStrs);
					tmpStrs[0] = new String(CellUtil.cloneRow(cell));
					tmpStrs[1] = String.valueOf(cell.getTimestamp());
					tmpStrs[2] = new String(CellUtil.cloneFamily(cell));
					tmpStrs[3] = new String(CellUtil.cloneQualifier(cell));
					tmpStrs[4] = new String(CellUtil.cloneValue(cell));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return Response.ok(retList);
	}
	
	@RequestMapping(value = "/scandata")
	public Response<List<List<String[]>>> scanData(String tableName, String startRow, String stopRow) {
		hbasec = new HBaseApi();
		List<Result> rs = null;
		List<List<String[]>> retList = new ArrayList<List<String[]>>();
		String[] tmpStrs = null;
		List<String[]> tmpList = null;
		try {
			rs = hbasec.scanData(tableName, startRow, stopRow);
			for (Result result : rs) {
				Cell[] cells = result.rawCells();
				tmpList = new ArrayList<String[]>(10);
				retList.add(tmpList);
				for (Cell cell : cells) {
					tmpStrs = new String[5];
					tmpList.add(tmpStrs);
					tmpStrs[0] = new String(CellUtil.cloneRow(cell));
					tmpStrs[1] = String.valueOf(cell.getTimestamp());
					tmpStrs[2] = new String(CellUtil.cloneFamily(cell));
					tmpStrs[3] = new String(CellUtil.cloneQualifier(cell));
					tmpStrs[4] = new String(CellUtil.cloneValue(cell));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return Response.ok(retList);
	}
	
}
