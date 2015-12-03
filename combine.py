import csv

input_file1 = open('3-output.csv')
input_file2 = open('train_v2.csv')
input_file3 = open('')
output_file = open('bow-output-labelled.csv', 'w')
csv_input = csv.DictReader(input_file1)
csv_input2 = csv.DictReader(input_file2)
csv_input3 = csv.DictReader(input_file3)

mapQuality = {}
fields1 = csv_input.fieldnames
fields2 = csv_input2.fieldnames
fields3 = csv_input3.fieldnames
fields = set(fields1)
fields = fields.union(set(fields2))
fields = fields.union(set(fields3)) 
writer = csv.DictWriter(output_file, fieldnames=fields)
for row in csv_input:
	for field in fields:
		if field not in row:
			row[field] = 0
	writer.writerow(row)

for row in csv_input2:
	for field in fields:
		if field not in row:
			row[field] = 0
	writer.writerow(row)


for row in csv_input3:
	for field in fields:
		if field not in row:
			row[field] = 0
	writer.writerow(row)



input_file1.close()
input_file2.close()
input_file3.close()
output_file.close()
