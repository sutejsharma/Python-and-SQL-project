import os, sys
from os.path import exists
from os import listdir
import csv
import psycopg2
import time
import shutil

#sets the root directory for the files that will be targeted by the script
rootdir = "C:\\Users\\Sutej\\Desktop\\Cycler_Data_2-10-2014\\Both\\" 


file2 = open("cycler_results.csv", "w")
file2.close()
textfile = open("sql statements.txt", "w")
textfile.close()

#Recording current time and date to create a new folder which has test files that have been executed by the script. 
current_time = str(time.strftime("%H_%M_%S"))
current_date = str(time.strftime("%Y-%m-%d"))

#Setting the path for the new folder that is created after the script is executed. This folder contains the test files that have been targeted by the script
newpath = r"C:\Users\Sutej\Desktop\Test_Uploaded_%s_%s"%(current_date,current_time)

#Checking if the new folder already exists and if not, then creates a new folder.
if not os.path.exists(newpath):
	os.makedirs(newpath)


for x in os.listdir(rootdir): #looping for all the test files in the source directory
	if(len(x) >= 11):
		if (x.find("aux_passfail") == (len(x) - 12)) and (x.find("aux_passfail")!= -1):
			rootdir = "C:\\Users\\Sutej\\Desktop\\Cycler_Data_2-10-2014\\Both\\"
			filename = x.split("_") #splitting the filename wherever there is an underscore character
			date = filename[0]
			partialtime = filename[1].split("h")
			hours = partialtime[0]
			partialtime2 = partialtime[1].split("m")
			minutes = partialtime2[0]
			partialtime3 = partialtime2[1].split("s")
			seconds = partialtime3[0]
			time = hours + ":" + minutes  + ":" + seconds
			serial_number = filename[2]
			bay_number= filename[3].lstrip("bay")
			
			complete_file_name = rootdir + x 
			f = open(complete_file_name,'r') #opening the test file
			file=f.read() #reading the contents of the test file
			f.close() #close the test file
			if "PASSED" in file: #checking to see if test result is pass or fail
				result="Pass"
			else:
				result="Fail"
			
			paramMaxImbal = "Max imbalance:"
			paramMaxImbalSpec = "mV (spec: 50 mV)"
			max_imbal = file[file.find(paramMaxImbal) + len(paramMaxImbal):file.find(paramMaxImbalSpec, file.find(paramMaxImbal) + len(paramMaxImbal))].strip() #retrieving max imbalance value from file
						
			paramMaxOver = "Max overestimation (last 15 minutes):"
			paramMaxOverSpec = "% (spec:"
			maxOver = file[file.find(paramMaxOver) + len(paramMaxOver):file.find(paramMaxOverSpec, file.find(paramMaxOver) + len(paramMaxOver))].strip() #retrieving max overestimation value from file
			
			paramTotal_mah = "Total mA-h:"
			paramTotal_mahSpec = "mAh"
			total_mah = file[file.find(paramTotal_mah) + len(paramTotal_mah):file.find(paramTotal_mahSpec, file.find(paramTotal_mah) + len(paramTotal_mah) )].strip() #retrieving total mili-amp-hours value from file
			
			paramTotal_cell_0 = "Total milliWatt-Hours, Cell0:"
			paramTotal_cell_0Spec = "mWh"
			total_cell_0 = file[file.find(paramTotal_cell_0) + len(paramTotal_cell_0):file.find(paramTotal_cell_0Spec, file.find(paramTotal_cell_0) + len(paramTotal_cell_0) )].strip()#retrieving Total milliWatt-Hours from file
			
			paramTotal_cell_1 = "Total milliWatt-Hours, Cell1:"
			paramTotal_cell_1Spec = "mWh"
			total_cell_1 = file[file.find(paramTotal_cell_1) + len(paramTotal_cell_1):file.find(paramTotal_cell_1Spec, file.find(paramTotal_cell_1) + len(paramTotal_cell_1) )].strip() #retrieving milliWatt-Hours for cell 1 from file
			
			paramTotal_cell_2 = "Total milliWatt-Hours, Cell2:"
			paramTotal_cell_2Spec = "mWh"
			total_cell_2 = file[file.find(paramTotal_cell_2) + len(paramTotal_cell_2):file.find(paramTotal_cell_2Spec, file.find(paramTotal_cell_2) + len(paramTotal_cell_2) )].strip()#retrieving milliWatt-Hours for cell 2 from file
			
			paramTotal_cell_3 = "Total milliWatt-Hours, Cell3:"
			paramTotal_cell_3Spec = "mWh"
			total_cell_3 = file[file.find(paramTotal_cell_3) + len(paramTotal_cell_3):file.find(paramTotal_cell_3Spec, file.find(paramTotal_cell_3) + len(paramTotal_cell_3) )].strip()#retrieving milliWatt-Hours for cell 3 from file

			row = date + "," + time + "," + serial_number + "," + bay_number + "," + result + "," + max_imbal + "," + maxOver + "," + total_mah + "," + total_cell_0 + "," + total_cell_1 + "," + total_cell_2 + "," + total_cell_3 + "\n"
			#row variable stores all the test information in a single row seperated by commas
			file1 = open("cycler_results.csv", "a+")
			file1.write(row) #All the test information for the file is entered as a row separated by commas in the excel CSV
			file1.close()
			destination = newpath
			shutil.move(complete_file_name,destination) # Now that the info from the test file has been extracted, its moved to a new folder so that there is no chance of it being executed again.
			
with open("cycler_results.csv", "r") as f: #open the CSV file that contains the test information
	for line in f: # loop through each line in the CSV
		linearray = line.split(",") # separate all the entries in the line into a list by using a comma delimiter
		pkey = -1
		textfile = open("sql statements.txt", "a+") #create a textfile that contains all the SQL statements (just a measure to test that the correct SQL statements are being generated)
		 		
		_sql = "SELECT partkey FROM aynserialtable WHERE partnumber = 'ASM-000518-02' AND serialnumber = 'SN'|| lpad($$" +linearray[2]+"$$, 6, '0')" #SQL statement with reference to linearray list that contains information from the CSV file
		textfile.write(_sql+ '\n') #write SQL query to the text file
		
		conn_string = "host='localhost' dbname='postgres' user='postgres' password='scat'"
		print "Connecting to database\n	->%s" % ("postgres")
		
		print "Connected!\n"
		
		conn = psycopg2.connect(conn_string)
		cursor = conn.cursor() #create a cursor object that is used to execute SQL statements on the server
		cursor.execute(_sql)
		query_return = cursor.fetchone()
		
		if (query_return is not None):
			pkey = int(query_return[0]) #retrieving part key information from query results
			conn.commit()
			cursor.close()
			conn.close()
			print "1"
		
		else:
			_sql = "SELECT partkey FROM aynserialtable WHERE partnumber = 'ASM-000518-01' AND serialnumber = 'SN'|| lpad($$"+ linearray[2]+"$$, 6, '0')"
			conn_string = "host='localhost' dbname='postgres' user='postgres' password='scat'"
			conn = psycopg2.connect(conn_string)
			cursor = conn.cursor()
			cursor.execute(_sql)
			query_return = cursor.fetchone()
			if ((query_return) is not None):
				pkey = int(query_return[0]) #retrieving part key information from query results
				conn.commit()
				cursor.close()
				conn.close()
				print "2"
				 
			else:
				_sql = "SELECT partkey FROM aynserialtable WHERE partnumber = 'ASM-000518-00' AND serialnumber = 'SN'|| lpad($$"+ linearray[2]+"$$, 6, '0')"
				conn_string = "host='localhost' dbname='postgres' user='postgres' password='scat'"
				conn = psycopg2.connect(conn_string)
				cursor = conn.cursor()
				cursor.execute(_sql)
				query_return = cursor.fetchone()
				if ((query_return) is not None):
					pkey = int(query_return[0]) #retrieving part key information from query results
					conn.commit()
					cursor.close()
					conn.close()
					print "3"
					
				else:
					pkey = -1
					print "4"
					
		
		if(pkey != -1): #checking that some partkey information was retrieved from the SQL query
			
			sql = "INSERT INTO ayn_t_entries(test_id, partkey, user_name, success, created, closed) VALUES(63,"+str(pkey)+",$$sutej1$$,$$"+linearray[4]+"$$,$$"+linearray[0]+" "+linearray[1]+"$$,$$"+linearray[0]+" "+linearray[1]+"$$)"
			textfile.write(sql+ '\n')
			conn_string = "host='localhost' dbname='postgres' user='postgres' password='scat'"
			conn = psycopg2.connect(conn_string)
			cursor = conn.cursor()
			cursor.execute(sql) #executes the SQL query stored in sql
			conn.commit()
			cursor.close()
			conn.close()
			
			sql = "SELECT MAX(id) FROM ayn_t_entries WHERE partkey = "+str(pkey)+""			
			textfile.write(sql+ '\n')
			conn_string = "host='localhost' dbname='postgres' user='postgres' password='scat'"
			conn = psycopg2.connect(conn_string)
			cursor = conn.cursor()
			cursor.execute(sql) #executes the SQL query stored in sql
			entry_id = cursor.fetchone()
			entry_id = entry_id[0]
			conn.commit()
			cursor.close()
			conn.close()
			
			sql = "INSERT INTO ayn_t_values(field_id, entry_id, value, created) VALUES(795,"+str(entry_id)+", $$"+linearray[0]+"$$, $$"+linearray[0]+" "+linearray[1]+"$$)"			
			textfile.write(sql+ '\n')
			conn_string = "host='localhost' dbname='postgres' user='postgres' password='scat'"
			conn = psycopg2.connect(conn_string)
			cursor = conn.cursor()
			cursor.execute(sql) #executes the SQL query stored in sql
			conn.commit()
			cursor.close()
			conn.close()

			sql = "INSERT INTO ayn_t_values(field_id, entry_id, value, created) VALUES(796, "+str(entry_id)+", $$"+linearray[1]+"$$, $$"+linearray[0]+" "+linearray[1]+"$$)"			
			textfile.write(sql+ '\n')
			conn_string = "host='localhost' dbname='postgres' user='postgres' password='scat'"
			conn = psycopg2.connect(conn_string)
			cursor = conn.cursor()
			cursor.execute(sql) #executes the SQL query stored in sql
			conn.commit()
			cursor.close()
			conn.close()

			sql = "INSERT INTO ayn_t_values(field_id, entry_id, value, created) VALUES(797, "+str(entry_id)+", $$"+linearray[3]+"$$, $$"+linearray[0]+" "+linearray[1]+"$$)"		
			textfile.write(sql+ '\n')
			conn_string = "host='localhost' dbname='postgres' user='postgres' password='scat'"
			conn = psycopg2.connect(conn_string)
			cursor = conn.cursor()
			cursor.execute(sql)#executes the SQL query stored in sql
			conn.commit()
			cursor.close()
			conn.close()

			sql = "INSERT INTO ayn_t_values(field_id, entry_id, value, created) VALUES(798, "+str(entry_id)+", $$"+linearray[5]+"$$, $$"+linearray[0]+" "+linearray[1]+"$$)"  			
			textfile.write(sql+ '\n')
			conn_string = "host='localhost' dbname='postgres' user='postgres' password='scat'"
			conn = psycopg2.connect(conn_string)
			cursor = conn.cursor()
			cursor.execute(sql) #executes the SQL query stored in sql
			conn.commit()
			cursor.close()
			conn.close()
			
			sql = "INSERT INTO ayn_t_values(field_id, entry_id, value, created) VALUES(799, "+str(entry_id)+", $$"+linearray[6]+"$$, $$"+linearray[0]+" "+linearray[1]+"$$)"			
			textfile.write(sql+ '\n')
			conn_string = "host='localhost' dbname='postgres' user='postgres' password='scat'"
			conn = psycopg2.connect(conn_string)
			cursor = conn.cursor()
			cursor.execute(sql) #executes the SQL query stored in sql
			conn.commit()
			cursor.close()
			conn.close()

			sql = "INSERT INTO ayn_t_values(field_id, entry_id, value, created) VALUES(800, "+str(entry_id)+", $$"+linearray[7]+"$$, $$"+linearray[0]+" "+linearray[1]+"$$)"
			textfile.write(sql+ '\n')
			conn_string = "host='localhost' dbname='postgres' user='postgres' password='scat'"
			conn = psycopg2.connect(conn_string)
			cursor = conn.cursor()
			cursor.execute(sql) #executes the SQL query stored in sql
			conn.commit()
			cursor.close()
			conn.close()


			sql = "INSERT INTO ayn_t_values(field_id, entry_id, value, created) VALUES(802, "+str(entry_id)+", $$"+linearray[8]+"$$, $$"+linearray[0]+" "+linearray[1]+"$$)"
			textfile.write(sql+ '\n')
			conn_string = "host='localhost' dbname='postgres' user='postgres' password='scat'"
			conn = psycopg2.connect(conn_string)
			cursor = conn.cursor()
			cursor.execute(sql) #executes the SQL query stored in sql
			conn.commit()
			cursor.close()
			conn.close()
			
			sql = "INSERT INTO ayn_t_values(field_id, entry_id, value, created) VALUES(803, "+str(entry_id)+", $$"+linearray[9]+"$$, $$"+linearray[0]+" "+linearray[1]+"$$)"
			textfile.write(sql+ '\n')
			conn_string = "host='localhost' dbname='postgres' user='postgres' password='scat'"
			conn = psycopg2.connect(conn_string)
			cursor = conn.cursor()
			cursor.execute(sql) #executes the SQL query stored in sql
			conn.commit()
			cursor.close()
			conn.close()

			sql = "INSERT INTO ayn_t_values(field_id, entry_id, value, created) VALUES(804, "+str(entry_id)+", $$"+linearray[10]+"$$, $$"+linearray[0]+" "+linearray[1]+"$$)"

			textfile.write(sql+ '\n')
			conn_string = "host='localhost' dbname='postgres' user='postgres' password='scat'"
			conn = psycopg2.connect(conn_string)
			cursor = conn.cursor()
			cursor.execute(sql) #executes the SQL query stored in sql
			conn.commit()
			cursor.close()
			conn.close()

			sql = "INSERT INTO ayn_t_values(field_id, entry_id, value, created) VALUES(805, "+str(entry_id)+", $$"+linearray[11].strip('\n')+"$$, $$"+linearray[0]+" "+linearray[1]+"$$)"

			textfile.write(sql+ '\n')
			conn_string = "host='localhost' dbname='postgres' user='postgres' password='scat'"
			conn = psycopg2.connect(conn_string)
			cursor = conn.cursor()
			cursor.execute(sql) #executes the SQL query stored in sql
			conn.commit()
			cursor.close()
			conn.close()
			

	textfile.close()
		
		
	
	
	
