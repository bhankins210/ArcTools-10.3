import arcpy
from arcpy import env

mdb_loc = r'H:\PROJECTS\SALES REPS\BRIAN HANKINS\requests'
mdb_name = 'projected_layers.gdb'
space = mdb_loc + '/' + mdb_name
arcpy.env.workspace = space

mxd = arcpy.mapping.MapDocument(r"CURRENT")
df = arcpy.mapping.ListDataFrames(mxd,'*')[0]
projection = "GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]]"

layers = arcpy.ListFeatureClasses(mxd)
for layer in layers:
	layer_name = str(layer) + '_projected'
	arcpy.Project_management(layer, layer_name,projection)
	