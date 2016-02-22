
import arcpy
from arcpy import env
from arcpy import mapping 
import time  
import pypyodbc

arcpy.AddMessage('\nDefining Variables')
time1 = time.clock()

#define input parameters
view_name_in = arcpy.GetParameterAsText(0)
view_name = view_name_in
# view_name = "svw" + view_name_in[20:]
table_name = "tbl" + view_name_in[3:]
# table_name = "tbl" + view_name_in[20:]


# define environment settings
space = r'C:\Users\bhankins\AppData\Roaming\ESRI\Desktop10.3\ArcCatalog\spatial_view.sde'
arcpy.env.workspace = space
mxd = arcpy.mapping.MapDocument(r"CURRENT")
df = arcpy.mapping.ListDataFrames(mxd,'*')[0]



connection_string = 'DRIVER={SQL Server};SERVER=mapping-sqldev\esri;DATABASE=spatial_view;UID=id;PWD=pass;Trusted_Connection=Yes'
con = pypyodbc.connect(connection_string)
cur = con.cursor()

sql_command = """DROP VIEW %s""" % view_name
sql_command_2 = """DROP TABLE %s""" % table_name
cur.execute(sql_command)
cur.execute(sql_command_2)
con.commit()
con.close()


# remove layer from dataframe
list_layer = arcpy.mapping.ListLayers(mxd,view_name_in,df)
for layer in list_layer:
	# if layer == view_name: 
	arcpy.mapping.RemoveLayer(df,layer)

target_folder = 'Database Connections\spatial_view.sde'	
arcpy.RefreshCatalog(target_folder)	
arcpy.RefreshTOC()
arcpy.RefreshActiveView()

