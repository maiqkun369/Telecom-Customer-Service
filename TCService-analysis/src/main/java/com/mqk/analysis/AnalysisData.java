package com.mqk.analysis;


import com.mqk.analysis.tool.AnalysisBeanTool;
import org.apache.hadoop.util.ToolRunner;

public class AnalysisData {
	public static void main(String[] args) {
		try {
			final int res = ToolRunner.run(new AnalysisBeanTool(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
