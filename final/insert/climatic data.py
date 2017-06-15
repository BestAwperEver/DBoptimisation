import os
import mysql.connector

files = os.listdir("./2015")

cnx = mysql.connector.connect(user='DBoptimisation', password='password',
                              host='radagast.asuscomm.com', database='climatic_data')
cursor = cnx.cursor()

for file in files:
	file = str(file)
	f = open("./2015/" + file)
	query = 'INSERT IGNORE INTO observations VALUES '
	line_count = 0
	for line in f:
		if line_count > 0:
			query += ','
		values = line.split()
		date = values[0] + '-' + values[1] + '-' + values[2]
		query += '(' + file.split('-')[0] + ',"' + date + '"'
		for value in values[3:-1]:
			query += ',' + value
		query += ')'
		line_count += 1
		# if line_count > 1000:
		# 	line_count = 0
		# 	query += ';'
		# 	cursor.execute(query)
		# 	query = 'INSERT IGNORE INTO observations VALUES '
	query += ';'
	cursor.execute(query)
	cnx.commit()
	f.close()
cursor.close()
cnx.close()
