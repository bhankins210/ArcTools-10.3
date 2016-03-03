import arcpy
from arcpy import env
from arcpy import mapping 


#Define variable through ArcMap script
arcpy.AddMessage('Defining Variables')
time1 = time.clock()
request_in = arcpy.GetParameterAsText(0)
request_id = request_in.upper() 
loc_out = request_id + '_LOC'
address_in = arcpy.GetParameterAsText(1)
city_in = arcpy.GetParameterAsText(2)
state_in = arcpy.GetParameterAsText(3)
zip_in = arcpy.GetParameter(4)
store_in = arcpy.GetParameterAsText(5)
buffer_name = request_id + "_Buffer"

# create new database
new_db = arcpy.GetParameterAsText(6)
new_mdb_loc = arcpy.GetParameterAsText(7)
new_mdb_name = arcpy.GetParameterAsText(8)

# use existing database
mdb_loc = arcpy.GetParameterAsText(9)

if str(new_db) == 'true':
	arcpy.CreatePersonalGDB_management(new_mdb_loc,new_mdb_name)
	print 'Database Crested'
	#arcpy.env.workspace = mdb_name
	space = new_mdb_loc + '/' + new_mdb_name
	arcpy.env.workspace = space
else: 
	space = mdb_loc
	arcpy.env.workspace = space
	arcpy.AddMessage('Use existing database')

# turn on buffering - default true
buffer_yes = arcpy.GetParameterAsText(10)
if str(buffer_yes) == 'true':
	run_buffer_tool = 'yes'
else:
	run_buffer_tool = 'no'
	
# override error out on zip centroid
centroid_error = arcpy.GetParameterAsText(11) 	
if str(centroid_error) == 'true':
	ignore_centroid_error = 'yes'
else:
	ignore_centroid_error = 'no'
	
# buffer parameters
buffer_in = arcpy.GetParameterAsText(12)
buffer_check = arcpy.GetParameterAsText(13)
if str(buffer_check) == 'true':
	buffer_out = str(buffer_in) + ' miles'
else:
	buffer_out = str(buffer_in)
dissolve_type = arcpy.GetParameterAsText(14)



if temp_table:
	arcpy.Delete_management(temp_table)
	arcpy.AddMessage('Deleting Temp Table')
	
table_loc = "in_memory"
table_name = "temp_table_name"
temp_table = arcpy.CreateTable_management(table_loc, table_name)
arcpy.AddField_management(temp_table, "Address", "TEXT", field_length=1100)
arcpy.AddField_management(temp_table, "city", "TEXT", field_length=30)
arcpy.AddField_management(temp_table, "state", "TEXT", field_length=2)
arcpy.AddField_management(temp_table, "zip", "TEXT", field_length=5)
arcpy.AddField_management(temp_table, "store", "TEXT", field_length=30)
arcpy.AddField_management(temp_table, "LATITUDE", "FLOAT", field_length=20)
arcpy.AddField_management(temp_table, "LONGITUDE", "FLOAT", field_length=20)
arcpy.AddField_management(temp_table, "FAILURECODE", "FLOAT", field_length=20)
arcpy.AddField_management(temp_table, "RADIUS", "FLOAT", field_length=20)
arcpy.AddField_management(temp_table, "REQUESTID", "FLOAT", field_length=20)


cursor = arcpy.da.InsertCursor(temp_table, ["Address", "city", "state", "zip", "store"])
cursor.insertRow([address_in, city_in, state_in, zip_in, store_in])

#Geocode location
arcpy.AddMessage('Geocoding Location')
time1 = time.clock()
address_locator = r'C:\ArcGIS\Business Analyst\US_2015\Data\Geocoding Data\USA_LocalComposite.loc'
address_fields = "Address Address;City City;State State;ZIP Zip"
geocode_result = loc_out
arcpy.GeocodeAddresses_geocoding(temp_table, address_locator, address_fields, geocode_result, 'STATIC')
arcpy.Delete_management(temp_table)
time2 = time.clock()  
arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")

#Add location to map document
arcpy.AddMessage('Adding Location Layer to TOC')
time1 = time.clock()
mxd = arcpy.mapping.MapDocument(r"CURRENT")
df = arcpy.mapping.ListDataFrames(mxd,'*')[0]

layer = arcpy.mapping.Layer(loc_out)
arcpy.mapping.AddLayer(df,layer)
time2 = time.clock()  
arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")


#Delete extra fields from location table
arcpy.AddMessage('Cleaning Location File')
time1 = time.clock()
layers = arcpy.mapping.ListLayers(mxd)
for layer in layers:
	if layer.name == loc_out:	
		temp_loc = loc_out

arcpy.CalculateField_management(temp_loc,"LATITUDE",'[Y]',"VB","#")
arcpy.CalculateField_management(temp_loc,"LONGITUDE",'[X]',"VB","#")
arcpy.CalculateField_management(temp_loc,"FAILURECODE","[Loc_name]","VB","#")

# drop_fields = ['loc_out','Status','Score','Match_type','Side','X','Y','Match_addr','Block','BlockL','BlockR','ARC_Addres','ARC_City','ARC_State','ARC_Zip','Address']
# arcpy.DeleteField_management(temp_loc,'loc_out;Status;Score;Match_type;Side;X;Y;Match_addr;Block;BlockL;BlockR;ARC_Addres;ARC_City;ARC_State;ARC_Zip')
# arcpy.DeleteField_management(loc_out,drop_fields)

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
		

