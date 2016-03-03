import arcpy
from arcpy import env
from arcpy import mapping 
import pypyodbc

# Specify Input variables

# location table - after geocoding
loc_table_in = arcpy.GetParameterAsText(0)
loc_table = loc_table_in.upper()

# # spatial view to profile
# spatial_view = arcpy.GetParameterAsText(1)

# Maximum Distance
max_radius = arcpy.GetParameterAsText(1)

# Handle duplication
dupl = arcpy.GetParameterAsText(2)


if str(dupl) == 'true':
	dup_on = 'yes'
else:
	dup_on = 'no'

arcpy.AddMessage('With Duplication - ' + dup_on)	

layer_list = list()
mxd = arcpy.mapping.MapDocument(r"Current")
view_lyr = arcpy.mapping.ListLayers(mxd,'*SVW*')
layer_list = list()

for layer in view_lyr:
	lyr_name = layer.name
	layer_list.append(lyr_name)

tbl_list = list()
for lyr in layer_list:
	if lyr[17:21].upper() == 'SVWZ':
		tbl_name = 'TBLZ_' + lyr[22:]
		print tbl_name
		tbl_list.append(tbl_name)
	if lyr[17:21].upper() == 'SVWS':
		tbl_name = 'TBLS_' + lyr[22:]
		print tbl_name
		tbl_list.append(tbl_name)
	if lyr[17:21].upper() == 'SVWR':
		tbl_name = 'TBLR_' + lyr[22:]
		print tbl_name
		tbl_list.append(tbl_name)
		
for table_view in tbl_list:
	spatial_view = table_view
	
	# Profile locations.  This calls either one (no dupliocation) or two (with duplication) stored procedures on MS-SQL2

	# If duplication box is checked:
	# First script profiles normally, without duplication.  2nd script adds duplication to max distance.	
			# First pass - no duplication
		# connect to MS-SQL2
	connection_string = 'DRIVER={SQL Server};SERVER=mapping-sqldev\esri;DATABASE=spatial_view;UID=id;PWD=pass;Trusted_Connection=Yes'
	con = pypyodbc.connect(connection_string)
	cur = con.cursor()	
	# Create search cursor in arcpy
	featureclass = loc_table
	rows = arcpy.SearchCursor(featureclass)
	row = rows.next()
	# Define variables for 
	radius = max_radius
	dupes = 'no'
	tbl = spatial_view
	# Loop through locations table and run stored procedure for each location.  This follows Arcmap's select logic.  
	# If features are selected, those are the only ones that will be profiled.
	while row:
		store = row.store
		lat = row.latitude
		lon = row.longitude
		# Build sql exec command
		sql_command = 'EXEC [dbo].[gis_SearchRadius_brian] ' + "'" + tbl + "'" + ', ' + "'" + str(lat) + "'" + ', ' + "'" +  str(lon) + "'" + ', ' + "'" + str(radius) + "'" +', ' + "'" + str(store) + "'" + ', ' + "'" + dupes + "'"
		cur.execute(sql_command)
		con.commit()
		row = rows.next()
	con.close()
	arcpy.AddMessage('Profile - First Pass')
	
	if dup_on == 'yes':

		
		# Second pass - with duplication 
		# connect to MS-SQL2
		connection_string = 'DRIVER={SQL Server};SERVER=mapping-sqldev\esri;DATABASE=spatial_view;UID=id;PWD=pass;Trusted_Connection=Yes'
		con = pypyodbc.connect(connection_string)
		cur = con.cursor()	
		# Create search cursor in arcpy
		featureclass = loc_table
		rows = arcpy.SearchCursor(featureclass)
		row = rows.next()
		# Define variables for 
		radius = max_radius
		dupes = 'no'
		tbl = spatial_view
		# Loop through locations table and run stored procedure for each location.  This follows Arcmap's select logic.  
		# If features are selected, those are the only ones that will be profiled.
		while row:
			store = row.store
			lat = row.latitude
			lon = row.longitude
			# Build sql exec command
			sql_command = 'EXEC [dbo].[gis_RadiusDupes_brian] ' + "'" + tbl + "'" + ', ' + "'" + str(lat) + "'" + ', ' + "'" +  str(lon) + "'" + ', ' + "'" + str(radius) + "'" +', ' + "'" + str(store) + "'" + ', ' + "'" + dupes + "'"
			cur.execute(sql_command)
			con.commit()
			row = rows.next()
		con.close()
		arcpy.AddMessage('Profile Complete')
		
		
