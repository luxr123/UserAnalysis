package com.tracker.weka;

import java.awt.BorderLayout;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Random;

import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.bayes.NaiveBayes;
import weka.classifiers.evaluation.ThresholdCurve;
import weka.core.Instances;
import weka.core.Utils;
import weka.gui.visualize.PlotData2D;
import weka.gui.visualize.ThresholdVisualizePanel;

public class SimpleROC {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		Instances data = new Instances(new BufferedReader(new FileReader("D:/segment-challenge.arff")));
		data.setClassIndex(data.numAttributes() - 1);

		Classifier cl = new NaiveBayes();
		Evaluation eval = new Evaluation(data);
		eval.crossValidateModel(cl, data, 10, new Random(1));

		ThresholdCurve tc = new ThresholdCurve();

		// classIndex is the index of the class to consider as "positive"
		int classIndex = 0;
		Instances result = tc.getCurve(eval.predictions(), classIndex);
		System.out.println("The area under the ROC curve: " + eval.areaUnderROC(classIndex));

		int tpIndex = result.attribute(ThresholdCurve.TP_RATE_NAME).index();
		int fpIndex = result.attribute(ThresholdCurve.FP_RATE_NAME).index();
		double[] tpRate = result.attributeToDoubleArray(tpIndex);
		double[] fpRate = result.attributeToDoubleArray(fpIndex);

		ThresholdVisualizePanel vmc = new ThresholdVisualizePanel();

		// 这个获得AUC的方式与上面的不同，其实得到的都是一个共同的结果
		vmc.setROCString("(Area under ROC = " + Utils.doubleToString(tc.getROCArea(result), 4) + ")");
		vmc.setName(result.relationName());
		PlotData2D tempd = new PlotData2D(result);
		tempd.setPlotName(result.relationName());
		tempd.addInstanceNumberAttribute();
		vmc.addPlot(tempd);

		// 显示曲面
		String plotName = vmc.getName();
		final javax.swing.JFrame jf = new javax.swing.JFrame("Weka Classifier Visualize: " + plotName);
		jf.setSize(500, 400);
		jf.getContentPane().setLayout(new BorderLayout());
		jf.getContentPane().add(vmc, BorderLayout.CENTER);
		jf.addWindowListener(new java.awt.event.WindowAdapter() {
			public void windowClosing(java.awt.event.WindowEvent e) {
				jf.dispose();
			}
		});
		jf.setVisible(true);
	}

}