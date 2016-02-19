import arcpy
from arcpy import env
from arcpy import mapping 
import csv
import os

#Define variable through ArcMap script
arcpy.AddMessage('Defining Variables')
time1 = time.clock()
request_in = arcpy.GetParameterAsText(0)
request_id = request_in.upper() 
loc_name = request_id + '_LOC'
address_in = arcpy.GetParameterAsText(1)
city_in = arcpy.GetParameterAsText(2)
state_in = arcpy.GetParameterAsText(3)
zip_in = arcpy.GetParameter(4)
store_in = arcpy.GetParameterAsText(5)
mdb_loc = arcpy.GetParameterAsText(6)
mdb_name = arcpy.GetParameterAsText(7)
ischecked = arcpy.GetParameterAsText(8)
if str(ischecked) == 'false':
	arcpy.CreatePersonalGDB_management(mdb_loc,mdb_name)
	print 'Database Crested'
	#arcpy.env.workspace = mdb_name
	space = mdb_loc + '/' + mdb_name
	arcpy.env.workspace = space
	arcpy.AddMessage('Creating New Database')
else: #in this case, the check box value is 'false', user did not check the box
	space = mdb_loc + '/' + mdb_name
	arcpy.env.workspace = space
	arcpy.AddMessage('Using Existing Database')
	
# turn on buffering - default true
buffer_yes = arcpy.GetParameterAsText(9)
if str(buffer_yes) == 'true':
	run_buffer_tool = 'yes'
else:
	run_buffer_tool = 'no'
	
# override error out on zip centroid
centroid_error = arcpy.GetParameterAsText(10) 	
if str(centroid_error) == 'true':
	ignore_centroid_error = 'yes'
else:
	ignore_centroid_error = 'no'
	
# buffer parameters
buffer_in = arcpy.GetParameterAsText(11)
buffer_check = arcpy.GetParameterAsText(12)
if str(buffer_check) == 'true':
	buffer_out = str(buffer_in) + ' miles'
else:
	buffer_out = str(buffer_in)
dissolve_type = arcpy.GetParameterAsText(13)


#build variable list to create temp location import table
lat = float('0')
long = float ('0')
loc_var = list()
loc_var.append(address_in)
loc_var.append(city_in)
loc_var.append(state_in)
loc_var.append(zip_in)
loc_var.append(store_in)
loc_var.append(lat)
loc_var.append(long)
time2 = time.clock()  
arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")

#Define scratch workspace
arcpy.AddMessage('Defining Workspaces')
time1 = time.clock()
arcpy.env.scratchWorkspace = r'C:\ArcGIS'
scratch = arcpy.env.scratchWorkspace
time2 = time.clock()  
arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")

#create temp location file
arcpy.AddMessage('Creating Location Import File')
time1 = time.clock()
loc_table_temp = scratch + '/' + 'temp_loc.csv'
with open(loc_table_temp,'wb') as out_file:
	writer = csv.DictWriter(out_file, fieldnames = ['Address','city','state','zip','store','LATITUDE','LONGITUDE','FAILURECODE','RADIUS','REQUESTID'], delimiter = ',')
	writer.writerow(dict(zip(writer.fieldnames, writer.fieldnames)))
	writer = csv.writer(out_file)
	writer.writerow(loc_var)
time2 = time.clock()  
arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")

#import location table	
arcpy.AddMessage('Importing Location')	
time1 = time.clock()
loc_table = 'loc_table'	
arcpy.TableToTable_conversion(loc_table_temp,space,loc_table)
time2 = time.clock()  
arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")

#Geocode location
arcpy.AddMessage('Geocoding Location')
time1 = time.clock()
address_table = loc_table_temp
# address_locator = r'C:\Program Files (x86)\ArcGIS\Desktop10.0\Business Analyst\Data\USA Geocoding Service\USA Geocoding Service.loc'
address_locator = r'C:\ArcGIS\Business Analyst\US_2015\Data\Geocoding Data\USA_LocalComposite.loc'
address_fields = "Address Address;City City;State State;ZIP Zip"
geocode_result = loc_name
arcpy.GeocodeAddresses_geocoding(address_table, address_locator, address_fields, geocode_result, 'STATIC')
time2 = time.clock()  
arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")

#Add location to map document
arcpy.AddMessage('Adding Location Layer to TOC')
time1 = time.clock()
mxd = arcpy.mapping.MapDocument(r"CURRENT")
df = arcpy.mapping.ListDataFrames(mxd,'*')[0]

layer = arcpy.mapping.Layer(loc_name)
arcpy.mapping.AddLayer(df,layer)
time2 = time.clock()  
arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")


#Delete extra fields from location table
arcpy.AddMessage('Cleaning Location File')
time1 = time.clock()
layers = arcpy.mapping.ListLayers(mxd)
for layer in layers:
	if layer.name == loc_name:	
		temp_loc = loc_name
	
arcpy.CalculateField_management(temp_loc,"LATITUDE",'[Y]',"VB","#")
arcpy.CalculateField_management(temp_loc,"LONGITUDE",'[X]',"VB","#")
arcpy.CalculateField_management(temp_loc,"FAILURECODE","[Loc_name]","VB","#")

# drop_fields = ['Loc_name','Status','Score','Match_type','Side','X','Y','Match_addr','Block','BlockL','BlockR','ARC_Addres','ARC_City','ARC_State','ARC_Zip','Address']
# arcpy.DeleteField_management(temp_loc,'Loc_name;Status;Score;Match_type;Side;X;Y;Match_addr;Block;BlockL;BlockR;ARC_Addres;ARC_City;ARC_State;ARC_Zip')
# arcpy.DeleteField_management(loc_name,drop_fields)
arcpy.Delete_management(loc_table)
time2 = time.clock()  
arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")

#Delete temp location file
arcpy.AddMessage('Removing Temp Files')
time1 = time.clock()
os.remove(loc_table_temp)
time2 = time.clock()  
arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")

if run_buffer_tool == 'yes':

	if ignore_centroid_error == 'no':
		arcpy.AddMessage('Checking for Zip Level Match')
		time1 = time.clock()
		#check for zip level match
		field = ['FAILURECODE']
		fc = loc_out
		cursor = arcpy.SearchCursor(fc)
		zip_match = '0'
		for row in cursor:
			addr_match = row.getValue(field[0])
			if addr_match != 'Zipcode': 
				continue
			if addr_match == 'Zipcode':
				zip_match = '1'
		time2 = time.clock()  
		arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")

		
		
		#Buffer if all address level match
		if zip_match != '1':

			#buffer
			arcpy.AddMessage('Buffering Locations')
			time1 = time.clock()
			arcpy.Buffer_analysis(loc_out,buffer_name,buffer_out,"FULL","ROUND",dissolve_type,"#")

			#add buffer to map
			layer = arcpy.mapping.Layer(buffer_name)
			arcpy.mapping.AddLayer(df,layer)

			arcpy.RefreshTOC()
			arcpy.RefreshActiveView()


			#select and print states that intersect the buffer
			arcpy.SelectLayerByLocation_management("Detailed\Geo Boundaries\States","INTERSECT",buffer_name)
			arcpy.AddMessage('Create Spatial View for these States:')
			for row in arcpy.SearchCursor("Detailed\Geo Boundaries\States"):
				states = row.NAME
				arcpy.AddMessage(states)
			time2 = time.clock()  
			arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")
				
		#error if any zip level match
		else:
			
			arcpy.AddMessage('ERROR!\nERROR!\nERROR!\n\nZip Code Level Match.  Please find correct Lat/Long before buffering\n\nERROR!\nERROR!\nERROR!\n')
		
	if ignore_centroid_error =='yes':
				#buffer
		arcpy.AddMessage('Buffering Locations')
		time1 = time.clock()
		arcpy.Buffer_analysis(loc_out,buffer_name,buffer_out,"FULL","ROUND",dissolve_type,"#")

		#add buffer to map
		layer = arcpy.mapping.Layer(buffer_name)
		arcpy.mapping.AddLayer(df,layer)

		arcpy.RefreshTOC()
		arcpy.RefreshActiveView()


		#select and print states that intersect the buffer
		arcpy.SelectLayerByLocation_management("Detailed\Geo Boundaries\States","INTERSECT",buffer_name)
		arcpy.AddMessage('Create Spatial View for these States:')
		for row in arcpy.SearchCursor("Detailed\Geo Boundaries\States"):
			states = row.NAME
			arcpy.AddMessage(states)
		time2 = time.clock()  
		arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")

if run_buffer_tool == 'no':
	arcpy.AddMessage('Checking for Zip Level Match')
	time1 = time.clock()
	#check for zip level match
	field = ['FAILURECODE']
	fc = loc_out
	cursor = arcpy.SearchCursor(fc)
	zip_match = '0'
	for row in cursor:
		addr_match = row.getValue(field[0])
		if addr_match != 'Zipcode': 
			continue
		if addr_match == 'Zipcode':
			zip_match = '1'
	time2 = time.clock()  
	arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")
	if zip_match != '1':
		arcpy.AddMessage('Geocoding Complete.  No Buffer Created')
			#error if any zip level match
	else:
		arcpy.AddMessage('ERROR!\nERROR!\nERROR!\n\nZip Code Level Match.  Please find correct Lat/Long before buffering\n\nERROR!\nERROR!\nERROR!\n')
		

