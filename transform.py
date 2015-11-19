import csv

input_file = open('3-output.csv')
label_file = open('train_v2.csv')
output_file = open('3-output-labelled.csv', 'w')
csv_input = csv.DictReader(input_file)
csv_label = csv.DictReader(label_file)

mapQuality = {}

for row in csv_label:
	mapQuality[row['file']] = row['sponsored']

quality = []

for row in csv_input:
	if row['filename'] in mapQuality.keys():
		quality.append(mapQuality[row['filename']])

i=0
j=0
input_file.close()

input_file = open('3-output.csv')

data = [list(line) for line in csv.reader(input_file, delimiter=",")]

input_file.close()

input_file = open('3-output.csv')

for line in input_file:
	if i==0:
		line = line.replace(r'\r\n','')
		content = line.strip().split(',')
		content.append('quality')
		strtemp = ','.join(content)
		output_file.write(strtemp+"\n")
	elif data[i][0] in mapQuality.keys():
		line = line.replace(r'\r\n','')
		content = line.strip().split(',')
		content.append(quality[j])
		strtemp = ','.join(content)
		output_file.write(strtemp+"\n")
	i = i+1
	j= j+1


output_file.close()
input_file.close()
label_file.close()
