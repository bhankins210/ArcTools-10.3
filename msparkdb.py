"""
database connection file

"""

import pypyodbc
import arcpy

def dbconn():
    try:
        connection = pypyodbc.connect('DRIVER={SQL Server};SERVER=mapping-sqldev\esri;DATABASE=spatial_view;UID=id;PWD=pass;Trusted_Connection=Yes')
        return connection
    except:
        print 'Cannot connect to database'

	
def cleanview(table_name):
    con = dbconn()
    cur = con.cursor()
    table_clean = "'" + table_name + "'"
    sql_command_5 = """EXEC [dbo].[gis_clean_view] %s""" % table_clean
    cur.execute(sql_command_5)
    con.commit()
    con.close()


# space = 'Database Connections\spatial_view.sde'
# arcpy.env.workspace = space
mxd = arcpy.mapping.MapDocument(r"CURRENT")
df = arcpy.mapping.ListDataFrames(mxd,'*')[0]	
db_connection = 'Database Connections\spatial_view.sde'
	
def importview(view_name):
	select_query = 'select * from ' + view_name
	arcpy.MakeQueryLayer_management(db_connection,view_name,select_query,"ID","POLYGON")
	layer = arcpy.mapping.Layer(view_name)
	arcpy.mapping.AddLayer(df, layer, "TOP")