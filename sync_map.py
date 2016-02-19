import arcpy
from arcpy import env
from arcpy import mapping 
import pypyodbc

# Specify Input variables

# specify view to sync
view_in = arcpy.GetParameterAsText(0)
view = view_in.upper()

# Define table name
view_table = 'spatial_view.dbo.tbl' + view[20:]


# Connection to ms-sql2
connection_string2 = 'DRIVER={SQL Server};SERVER=mapping-sqldev\esri;DATABASE=spatial_view;UID=id;PWD=pass;Trusted_Connection=Yes'
con2 = pypyodbc.connect(connection_string2)
cur2 = con2.cursor()

# Create search cursor in arcpy
featureclass = view
rows = arcpy.SearchCursor(featureclass)
row = rows.next()

# Set all previous table selections to 0
sql_command = """UPDATE %s SET selected = 0""" % view_table
cur2.execute(sql_command)
con2.commit()

# Loop through selected features and set "selected" field to 1 
while row:
	zip = row.zip
	zip_list = list()
	zip_list.append(zip)
	sql_command = """UPDATE %s SET selected = 1 WHERE zip = ?""" % view_table
	cur2.execute(sql_command,zip_list)
	con2.commit()
	row = rows.next()
#con2.close()

# Delete table records if "selected" = 0
sql_command = """DELETE FROM %s WHERE selected = 0""" % view_table
cur2.execute(sql_command)
con2.commit()


con2.close()

# arcpy.SelectLayerByAttribute_management(view_table,"SWITCH_SELECTION")

# with arcpy.da.UpdateCursor(view_table, "OBJECTID") as cursor:
	# for row in cursor:
		# cursor.deleteRow()
		
