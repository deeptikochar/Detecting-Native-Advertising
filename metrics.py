from __future__ import division
from collections import defaultdict


#Calculate Accuracy
def getErrorPercentage(predicted,true):
    count=0
    for i,j in zip(true,predicted):
        if i==j: count+=1
        #confusion[i,j] += 1
    return (float)(count)/len(true)#,confusion


#Calculate precision and recall 
def getPrecisionandRecall(predictedLabel,trueLabels):
	# print len(predictedLabel), len(trueLabels)
	tp = 0.0
	fp = 0.0
	fn = 0.0
	tn = 0.0
	# precision = defaultdict(float)
	# recall = defaultdict(float)
	# fmeasure = defaultdict(float)
	uniqLabels = set(trueLabels)

	if len(uniqLabels) > 2:
		print 'Precision and Recall are only defined for binary classifiers'

	for predicted,actual in zip(predictedLabel,trueLabels):
		if actual == 1:
			if predicted == actual:
				tp += 1
			else:
				fp += 1
		if actual == 0:
			if predicted != actual:
				fn += 1
			else:
				tn += 1

	print tp, tn, fp, fn
	# raw_input()
	precision = tp/(0.0000000001+tp+fp)
	recall = tp/(0.0000000001+tp+fn)
	fmeasure = 2*precision*recall/(0.0000000001+precision + recall)

	# precision[0] = tn/(tn+fn)
	# recall[0] = tn/(tn+fp)
	# fmeasure[0] = 2*precision[0]*recall[0]/(precision[0] + recall[0])

	return (precision,recall,fmeasure)
