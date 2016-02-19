import arcpy
from arcpy import env
from arcpy import mapping 
import time  
import pypyodbc




zip = arcpy.GetParameterAsText(0)

zip2 = arcpy.GetParameterAsText(1)



arcpy.AddMessage(zip)
if str(zip) == 'true':
	zip_on = 'yes'
else:
	zip_on = 'no'

arcpy.AddMessage(zip2)
if str(zip2) == 'true':
	zip2_on = 'yes'
else:
	zip2_on = 'no'
	