package com.mqk.analysis;


import com.mqk.analysis.tool.AnalysisTextTool;
import org.apache.hadoop.util.ToolRunner;

public class AnalysisData {
	public static void main(String[] args) {
		try {
			final int res = ToolRunner.run(new AnalysisTextTool(), args);


		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
