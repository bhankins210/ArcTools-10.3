import arcpy
from arcpy import env
import os

#define script parameters
arcpy.AddMessage('Defining Variables')
time1 = time.clock()
request_in = arcpy.GetParameterAsText(0)
request_id = request_in.upper() 
loc_name = arcpy.GetParameterAsText(1)
loc_in = loc_name + '\SHEET1$'
mdb_loc = arcpy.GetParameterAsText(2)
mdb_name = arcpy.GetParameterAsText(3)
ischecked = arcpy.GetParameterAsText(4)
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
# buffer_in = arcpy.GetParameterAsText(5)
# buffer_check = arcpy.GetParameterAsText(6)
# if str(buffer_check) == 'true':
	# buffer_out = str(buffer_in) + ' miles'
# else:
	# buffer_out = str(buffer_in)
# dissolve_type = arcpy.GetParameterAsText(7)


#define variables
request_folder = request_id
loc_out = request_id + "_LOC"
loc_table = 'loc_table'
# buffer_name = request_id + "_Buffer"
xy_event = 'xy_event'
mxd = arcpy.mapping.MapDocument(r"CURRENT")
df = arcpy.mapping.ListDataFrames(mxd,'*')[0]
time2 = time.clock()  
arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")

#create location table
arcpy.AddMessage('Importing Locations')
time1 = time.clock()
arcpy.TableToTable_conversion(loc_in,space,loc_table)
time2 = time.clock()  
arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")

#Geocode location
arcpy.AddMessage('Geocoding Locations')
time1 = time.clock()
address_table = loc_table
# address_locator = r'C:\Program Files (x86)\ArcGIS\Desktop10.0\Business Analyst\Data\USA Geocoding Service\USA Geocoding Service.loc'
address_locator = r'C:\ArcGIS\Business Analyst\US_2015\Data\Geocoding Data\USA_LocalComposite.loc'
address_fields = 'address address;city city;state state;zip zip'
geocode_result = loc_out
arcpy.GeocodeAddresses_geocoding(address_table, address_locator, address_fields, geocode_result, 'STATIC')
time2 = time.clock()  
arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")

#Add location to map document
arcpy.AddMessage('Adding Location Layer to TOC')
time1 = time.clock()
layer = arcpy.mapping.Layer(loc_out)
arcpy.mapping.AddLayer(df,layer)
time2 = time.clock()  
arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")

#Delete extra fields from location table
# temp_loc = 'temp_loc'
arcpy.AddMessage('Cleaning Location File')
time1 = time.clock()
layers = arcpy.mapping.ListLayers(mxd)
for layer in layers:
	if layer.name == loc_out:	
		temp_loc = loc_out


arcpy.CalculateField_management(temp_loc,"LATITUDE",'[Y]',"VB","#")
arcpy.CalculateField_management(temp_loc,"LONGITUDE",'[X]',"VB","#")
arcpy.CalculateField_management(temp_loc,"FAILURECODE","[Loc_name]","VB","#")
#drop_fields = ['Loc_name','Status','Score','Match_type','Side','X','Y','Match_addr','Block','BlockL','BlockR','ARC_Addres','ARC_City','ARC_State','ARC_Zip','Address']
# arcpy.DeleteField_management(temp_loc,'Loc_name;Status;Score;Match_type;Side;X;Y;Match_addr;Block;BlockL;BlockR;ARC_Addres;ARC_City;ARC_State;ARC_Zip')
#arcpy.DeleteField_management(loc_out,drop_fields)
arcpy.Delete_management(loc_table)
time2 = time.clock()  
arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")




# arcpy.AddMessage('Checking for Zip Level Match')
# time1 = time.clock()
# #check for zip level match
# field = ['FAILURECODE']
# fc = loc_out
# cursor = arcpy.SearchCursor(fc)
# zip_match = '0'
# for row in cursor:
	# addr_match = row.getValue(field[0])
	# if addr_match != 'Zipcode': 
		# continue
	# if addr_match == 'Zipcode':
		# zip_match = '1'
# time2 = time.clock()  
# arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")
		
# #Buffer if all address level match
# if zip_match != '1':

	# #buffer
	# arcpy.AddMessage('Buffering Locations')
	# time1 = time.clock()
	# arcpy.Buffer_analysis(loc_out,buffer_name,buffer_out,"FULL","ROUND",dissolve_type,"#")

	# #add buffer to map
	# layer = arcpy.mapping.Layer(buffer_name)
	# arcpy.mapping.AddLayer(df,layer)

	# arcpy.RefreshTOC()
	# arcpy.RefreshActiveView()


	# #select and print states that intersect the buffer
	# arcpy.SelectLayerByLocation_management("Detailed\Geo Boundaries\States","INTERSECT",buffer_name)
	# arcpy.AddMessage('Create Spatial View for these States:')
	# for row in arcpy.SearchCursor("Detailed\Geo Boundaries\States"):
		# states = row.NAME
		# arcpy.AddMessage(states)
	# time2 = time.clock()  
	# arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")
		
# #error if any zip level match
# else:
	
	# arcpy.AddMessage('ERROR!\nERROR!\nERROR!\n\nZip Code Level Match.  Please find correct Lat/Long before buffering\n\nERROR!\nERROR!\nERROR!\n')
	

	
