
import arcpy
from arcpy import env
from arcpy import mapping 
import time  
import pypyodbc

arcpy.AddMessage('\nDefining Variables')
time1 = time.clock()

#define input parameters
view_name_in = arcpy.GetParameterAsText(0)
# view_name = view_name_in
view_space = view_name_in.find('svw')
view_name = view_name_in[view_space:]

table_name = "tbl" + view_name[3:]
# table_name = "tbl" + view_name_in[20:]


# define environment settings
space = r'C:\Users\bhankins\AppData\Roaming\ESRI\Desktop10.3\ArcCatalog\spatial_view.sde'
arcpy.env.workspace = space
mxd = arcpy.mapping.MapDocument(r"CURRENT")
df = arcpy.mapping.ListDataFrames(mxd,'*')[0]


# execute sql commands
connection_string = 'DRIVER={SQL Server};SERVER=mapping-sqldev\esri;DATABASE=spatial_view;UID=id;PWD=pass;Trusted_Connection=Yes'
con = pypyodbc.connect(connection_string)
cur = con.cursor()
sql_command = """SELECT  zip, MIN(distance) as distance INTO #dist FROM %s GROUP BY zip""" % table_name
sql_command_2 = """DELETE FROM %s WHERE (zip + distance) NOT IN (SELECT zip + distance FROM #dist) DROP TABLE #dist""" % table_name
sql_command_3 = """UPDATE  %s SET distance = NULL""" % table_name
sql_command_4 = """UPDATE %s SET store = NULL""" % table_name
cur.execute(sql_command)
cur.execute(sql_command_2)
cur.execute(sql_command_3)
cur.execute(sql_command_4)
con.commit()
con.close()

# refresh geodatabase
target_folder = 'Database Connections\spatial_view.sde'	
arcpy.RefreshCatalog(target_folder)	
arcpy.RefreshTOC()
arcpy.RefreshActiveView()
