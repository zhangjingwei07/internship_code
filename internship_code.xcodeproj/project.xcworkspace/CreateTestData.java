package com.dinfo.bigdata;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Date;

import org.junit.Test;

public class CreateTestData {
	
	@Test
	public void c() throws FileNotFoundException {
		String s = "14944949521740000	{\"id\":\"3\",\"content\":\"我爱中国\"}";
		PrintWriter pw = new PrintWriter("E:/test/test.txt");
		long p = 0L, l = 0L, n = 0L, maxc = 0L, tmp = 0L;
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < 200000; i++) {
			l = new Date().getTime();
			sb = new StringBuffer();
			sb.append(getFixLenthString(p, l)).append("\t");
			sb.append("{\"id\":\"").append(i);
			sb.append("\",\"content\":\"").append("test").append(i).append("\"}");
			pw.println(sb.toString());
			p = l;
			if(i % 10000 == 0){
				System.out.println(i);
			}
		}
		pw.close();
	}
	
	long y = 0L;
	
	private String getFixLenthString(long p, long l) {
		y = l == p ? ++y : (y = 0);
		// 0 代表前面补充0
		// 4 代表长度为4
		// d 代表参数为正数型
		return l + String.format("%04d", y);
	}
	
}
