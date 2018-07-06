package com.dinfo.bigdata.web;

import com.dinfo.bigdata.hdfs.FileReadFromHdfs;
import com.dinfo.common.model.Response;
import org.apache.log4j.Logger;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/hdfs")
public class HdfsWeb {
	
	private final Logger logger = Logger.getLogger(getClass());
	private FileReadFromHdfs hdfsr = null;
	
	@RequestMapping(value = "/filestatus", method = RequestMethod.GET)
	public Response<Integer> checkHdfsFileStatus(@RequestParam String filePath) {
		hdfsr = new FileReadFromHdfs();
		if (!hdfsr.isfile(filePath)) {
			return Response.notOk("文件路径不存在");
		}
		if (hdfsr.isDirectory(filePath)) {
			return Response.ok(2);
		} else {
			return Response.ok(1);
		}
	}
	
	@RequestMapping(value = "/querydata", method = RequestMethod.GET)
	public Response<List<String>> queryData(@RequestParam String path, @RequestParam Long rowNum) {
		hdfsr = new FileReadFromHdfs();
		List<String> ret = hdfsr.read(rowNum, path);
		return Response.ok(ret);
	}
	
	@RequestMapping(value = "/querycount", method = RequestMethod.GET)
	public Response<Long> queryCount(@RequestParam String path) {
		hdfsr = new FileReadFromHdfs();
		long ret = hdfsr.count(path);
		return Response.ok(ret);
	}
	
	@RequestMapping(value = "/filelength", method = RequestMethod.GET)
	public Response<Long> fileLength(@RequestParam String path) {
		hdfsr = new FileReadFromHdfs();
		long ret = hdfsr.fileLength(path);
		return Response.ok(ret);
	}
	
	@RequestMapping(value = "/readtowrite", method = RequestMethod.GET)
	public Response<Long> readToWrite(@RequestParam String inPath, @RequestParam String outPath) {
		hdfsr = new FileReadFromHdfs();
		long ret = hdfsr.readToWrite(inPath, outPath, null);
		return Response.ok(ret);
	}

	/**
	 * 读入hdfs文件，落地成指定分片数量的hdfs文件
	 * @param inPaths 输入文件路径列表
	 * @param outPath 输出文件父路径
	 * @param taskkey 流水号
	 * @param splitNum 分片数量
	 * @return json字符串格式如：{"count":50, "outFileList":["split0","split1",...]}
	 *         输出文件列表(outFileList)不包含文件路径
	 */
	@RequestMapping(value = "/hdfsLanding", method = RequestMethod.GET)
	public Response<String> hdfsLanding(	@RequestParam List<String> inPaths, @RequestParam String outPath,
										@RequestParam String taskkey, @RequestParam Integer splitNum) {
		hdfsr = new FileReadFromHdfs();
		String ret = hdfsr.hdfsLanding(inPaths, outPath, taskkey, splitNum);
		if("".equals(ret)){
			//有异常发生
			Response.notOk("hdfsr.hdfsLanding has a Exception !");
		}
		return Response.ok(ret);
	}

	@RequestMapping(value = "/readtowritetask", method = RequestMethod.GET)
	public Response<Long> readToWrite(	@RequestParam String inPath, @RequestParam String outPath,
										  @RequestParam String taskkey) {
		hdfsr = new FileReadFromHdfs();
		long ret = hdfsr.readToWrite(inPath, outPath, taskkey);
		return Response.ok(ret);
	}

	@RequestMapping(value = "/getfilelists", method = RequestMethod.GET)
	public Response<List<String>> getFileLists(@RequestParam String path) {
		hdfsr = new FileReadFromHdfs();
		List<String> ret = hdfsr.getFileLists(path);
		return Response.ok(ret);
	}
	
	@RequestMapping(value = "/gpsqlwrite", method = RequestMethod.GET)
	public Response<String> gpSqlWrite(	@RequestParam String url, @RequestParam String user, @RequestParam String pass,
										@RequestParam String sql, @RequestParam String hdfsPath,
										@RequestParam String retField) {
		hdfsr = new FileReadFromHdfs();
		String ret = hdfsr.gpSqlWrite(url, user, pass, sql, hdfsPath, retField, null);
		return Response.ok(ret);
	}
	
	@RequestMapping(value = "/gpsqlwritetask", method = RequestMethod.GET)
	public Response<String> gpSqlWrite(	@RequestParam String url, @RequestParam String user, @RequestParam String pass,
										@RequestParam String sql, @RequestParam String hdfsPath,
										@RequestParam String retField, @RequestParam String taskkey) {
		hdfsr = new FileReadFromHdfs();
		String ret = hdfsr.gpSqlWrite(url, user, pass, sql, hdfsPath, retField, taskkey);
		return Response.ok(ret);
	}
	
	/**
	 * 将greenplum数据记录落地为hdfs文件
	 * @param url 数据源连接url
	 * @param user 用户名
	 * @param pass 密码
	 * @param sql sql语句
	 * @param hdfsPath 输出文件父路径
	 * @param retField 自增字段名
	 * @param taskkey 批次流水号
	 * @param splitNum 分片数量
	 * @return json字符串格式如：{"max":1000, "count":50, "outFileList":["split0","split1",...]}
	 *         输出文件列表(outFileList)不包含文件路径
	 */
	@RequestMapping(value = "/gpsqlLanding", method = RequestMethod.GET)
	public Response<String> gpsqlLanding(	@RequestParam String url, @RequestParam String user, @RequestParam String pass,
										   @RequestParam String sql, @RequestParam String hdfsPath,
										   @RequestParam String retField, @RequestParam String taskkey,
										   @RequestParam Integer splitNum) {
		hdfsr = new FileReadFromHdfs();
		String ret = hdfsr.gpsqlLanding(url, user, pass, sql, hdfsPath, retField, taskkey, splitNum);
		if("".equals(ret)){
			//有异常发生
			Response.notOk("hdfsr.gpsqlLanding has a Exception !");
		}
		return Response.ok(ret);
	}

	@RequestMapping(value = "/dels")
	public Response<Boolean> dels(@RequestBody Response response) {
		hdfsr = new FileReadFromHdfs();
		List<String> listPath = (List<String>) response.getData();
		boolean ret = hdfsr.dels(listPath);
		return Response.ok(ret);
	}
	
	@RequestMapping(value = "/upFile", method = RequestMethod.POST)
	public Response<Boolean> upFile(@RequestBody Response<byte[]> response,@RequestParam String outPath) {
		hdfsr = new FileReadFromHdfs();
		Boolean upFile = hdfsr.upFile(response.getData(), outPath);
		return Response.ok(upFile);
	}
	
	@RequestMapping(value = "/downFile", method = RequestMethod.POST)
	public Response<byte[]> downFile(@RequestParam String filePath) {
		hdfsr = new FileReadFromHdfs();
		byte[] downFile = hdfsr.downFile(filePath);
		return Response.ok(downFile);
	}
	
}
