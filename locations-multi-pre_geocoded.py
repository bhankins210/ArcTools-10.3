# import pre-geocoded locations table and add to TOC


import arcpy
from arcpy import env


# input parameters
request_in = arcpy.GetParameterAsText(0)
request_id = request_in.upper() 
loc_name = arcpy.GetParameterAsText(1)
loc_in = loc_name + '\SHEET1$'

# create new database
new_db = arcpy.GetParameterAsText(2)
new_mdb_loc = arcpy.GetParameterAsText(3)
new_mdb_name = arcpy.GetParameterAsText(4)

# existing_db = arcpy.GetParameterAsText(5)
mdb_loc = arcpy.GetParameterAsText(5)

if str(existing_db) == 'false':
	arcpy.CreatePersonalGDB_management(new_mdb_loc,new_mdb_name)
	print 'Database Crested'
	#arcpy.env.workspace = mdb_name
	space = new_mdb_loc + '/' + new_mdb_name
	arcpy.env.workspace = space
else: 
	space = mdb_loc
	arcpy.env.workspace = space
	arcpy.AddMessage('Use existing database')

# define variables
request_folder = request_id
loc_out = request_id + "_LOC"
loc_table = 'loc_table'
xy_event = 'xy_event'
mxd = arcpy.mapping.MapDocument(r"CURRENT")
df = arcpy.mapping.ListDataFrames(mxd,'*')[0]

#create location table
arcpy.TableToTable_conversion(loc_in,space,loc_table)
sr = r'C:\Program Files (x86)\ArcGIS\Desktop10.0\Coordinate Systems\Geographic Coordinate Systems\World\WGS 1984.prj'
arcpy.MakeXYEventLayer_management(loc_in,"LONGITUDE","LATITUDE",xy_event,sr)
arcpy.CopyFeatures_management(xy_event,loc_out)
arcpy.Delete_management(xy_event)
arcpy.Delete_management(loc_table)

# add layer to toc and refresh view
layer2 = arcpy.mapping.Layer(loc_out)
arcpy.mapping.AddLayer(df,layer2)
arcpy.RefreshTOC()
arcpy.RefreshActiveView()
