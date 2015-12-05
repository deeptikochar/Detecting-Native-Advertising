from __future__ import division
import numpy as np
from numpy import genfromtxt
from sklearn.linear_model import LogisticRegression as LR
from sklearn.tree import DecisionTreeClassifier as DT
from sklearn.svm import LinearSVC as SVM
from sklearn.ensemble import RandomForestClassifier as RF

import pickle
import metrics
from sklearn.metrics import accuracy_score,f1_score
from sklearn.cross_validation import KFold
from random import shuffle

from nltk.stem import PorterStemmer
from nltk.corpus import stopwords
import re
from collections import defaultdict

from sklearn.feature_selection import RFE
# from sklearn.feature_selection import SelectFromModel
from sklearn.ensemble import ExtraTreesClassifier as ETC
from sklearn.ensemble import AdaBoostClassifier as ADA
from sklearn.ensemble import GradientBoostingClassifier as G

stop = stopwords.words('english')
lmtzr = PorterStemmer()
numberPattern = re.compile("[0-9]+")
punctpattern = re.compile("[?!.-;,:]+")

FEATS1 = ['word_count','line_count','space_count','tab_count','brace_count','bracket_count','length']
FEATS2 = ['form_count','div_count','samp_count','ol_count','h3_count','link_count','figcaption_count','object_count','u_count','track_count','dd_count','code_count','h6_count','h4_count','li_count','em_count','progress_count','paragraph_count','select_count','header_count','button_count','caption_count','sub_count','dir_count','small_count','dialog_count','time_count','map_count','col_count','main_count','input_count','style_count','tr_count','dt_count','dl_count','bold_count','tfoot_count','section_count','ins_count','source_count','td_count','output_count','h2_count','span_count','aside_count','textarea_count','blockquote_count','datalist_count','svg_count','mark_count','iframe_count','image_count','wbr_count','del_count','tbody_count','hr_count','meter_count','footer_count','colgroup_count','optgroup_count','article_count','thead_count','a_count','abbr_count','br_count','font_count','option_count','table_count','legend_count','th_count','area_count','noscript_count','script_count','canvas_count','keygen_count','bdi_count','video_count','h5_count','summary_count','pre_count','s_count','quotation_count','fieldset_count','audio_count','strong_count','bdo_count','address_count','nav_count','menu_count','label_count','kbd_count','h1_count','dfn_count','sup_count','cite_count','embed_count','details_count','i_count','var_count','figure_count','param_count','menuitem_count','ul_count']
FEATS3 = ['distinct_domains','total_urls']
FEATS = FEATS1 +FEATS2 +FEATS3

FOLDS = 10

def removeNanColumn(a):
	for i in xrange(len(a[0])-1):
		if np.isnan(a[0][i]):
			print "Removing Nan from", i
			a = np.delete(a,i,1)
			# a[0][i] = 0
			break
	return a

def analyzeFeatures(model, xTrain, yTrain):
	model.fit(xTrain, yTrain)
	# display the relative importance of each attribute
	imp = model.feature_importances_
	inds = sorted(range(len(imp)), key=lambda k: imp[k])
	print imp, inds
	bestFeats = []
	for i in inds:
		bestFeats.append((FEATS[i],imp[i]))
	for b in bestFeats:
		print b[0], '\t', b[1]
	return np.delete(xTrain,inds[0:-20],axis=1)

def splitData(xData,yData):
	kf = KFold(len(xData), n_folds=10)
	for train,test in kf:
		xTrain,yTrain,xTest,yTest = xData[train],yData[train],xData[test],yData[test]
		yield xTrain,yTrain,xTest,yTest

def testTrainSplit(xData,yData):

	xTrain,xTest = np.split(xData, [int((60/100)*len(xData))])
	yTrain,yTest = np.split(yData, [int((60/100)*len(xData))])

	return xTrain,yTrain,xTest,yTest	

def trainClassifier(xTrain,yTrain):
	# learner = LR(penalty='l2')
	# learner = SVM()
	# learner = DT()
	# learner = RF()
	learner = ETC()
	# learner = ADA(n_estimators=200)
	# learner = G(n_estimators=100)
	learner.fit(xTrain,yTrain)
	return learner

def loadData(arg, X_curr=None):
	filepath, cols1, labelCol = arg
	X = genfromtxt(filepath, delimiter=',', skip_header=1, usecols=cols1, skip_footer=2)
	if X_curr!=None:
		X = np.concatenate((X_curr, X), axis=1)
	y = genfromtxt(filepath, delimiter=',', skip_header=1, usecols=labelCol, skip_footer=2)
	return X,y

def loadTest():
	x = genfromtxt('data/test.csv', delimiter=',', skip_header=1, usecols=range(7))
	return x

def getSet1():
	filePath = ('data/all_train.csv',range(7),[-1])
	xTrain,yTrain = loadData(filePath)
	filePath = ('html_data/all_train.csv',range(103),[-1])
	xTrain,yTrain = loadData(filePath, xTrain)
	return xTrain,yTrain

def getSet2():
	filePath = ('data/all_train.csv',range(7),[-1])
	xTrain,yTrain = loadData(filePath)
	filePath = ('html_data/all_train.csv',range(103),[-1])
	xTrain,yTrain = loadData(filePath, xTrain)
	xTrain,yTrain = xTrain[0:4062] ,yTrain[0:4062]
	# print sum(yTrain)
	filePath = ('domain_data/all_train.csv',[455,5207],[-1])
	xTrain,yTrains = loadData(filePath,xTrain)
	return xTrain,yTrain

def getSetBow():
	# bow-seema-labelled
	# filePath = ('html_data/bow-seema-labelled.csv',None,[-1])
	filePath = ('html_data/3_bow_small.csv',None,[-1]) #53016
	xTrain,yTrain = loadData(filePath)
	xTrain = removeNanColumn(xTrain)
	# yTrain = removeNanColumn(yTrain)
	xTrain = np.delete(xTrain,len(xTrain[0])-1,1)
	return xTrain,yTrain

def main():

	xTrain,yTrain = getSet1()
	# xTrain,yTrain = getSet2()

	print xTrain.shape
	print xTrain,yTrain, sum(yTrain)
	# raw_input()

	# xTrain = analyzeFeatures(ETC(), xTrain, yTrain)
	# analyzeFeatures(ETC(), xTrain, yTrain)
	# print xTrain.shape
	# print xTrain,yTrain
	# return

	xTest = loadTest()

	bestClassifier = None
	minF = 0
	P = R = F = 0
	i = 1
	for xCVTrain,yCVTrain,xCVTest,yCVTest in splitData(xTrain,yTrain):
		print "\nCross Validation: Training and testing instance", i
		#xTrain,yTrain,xTest,yTest = splitData(xData,yData)
		classifier = trainClassifier(xCVTrain,yCVTrain)
		Y_1 = classifier.predict(xCVTest)
		# Y_1 = np.array([0]*len(yCVTest))
		ret = metrics.getPrecisionandRecall(Y_1,yCVTest)
		P += ret[0]
		R += ret[1]
		F += ret[2]
		i+=1

	print 'Accuracy - ', accuracy_score(yCVTest,Y_1)
	# print 'P R F  - ', P/FOLDS,R/FOLDS,F/FOLDS

def main2():
	xTrain,yTrain = getSetBow()
	classifier = trainClassifier(xTrain,yTrain)
	Y_1 = classifier.predict(xCVTest)
	# Y_1 = np.array([0]*len(yCVTest))
	ret = metrics.getPrecisionandRecall(Y_1,yCVTest)
	P += ret[0]
	R += ret[1]
	F += ret[2]

	print 'Accuracy - ', accuracy_score(yCVTest,Y_1)


if __name__ == '__main__':
	main()