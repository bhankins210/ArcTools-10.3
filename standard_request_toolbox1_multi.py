'''----------------------------------------------------------------------------------
 Tool Name:   Multiple Ring Buffer
 Source Name: MultiRingBuffer.py
 Version:     ArcGIS 10.0
 Author:      Environmental Systems Research Institute Inc.
 Required Arguments:
              An input feature class or feature layer
              An output feature class
              A set of distances (multiple set of double values)
 Optional Arguments:
              The name of the field to contain the distance values (default="distance")
              Option to have the output dissolved (default="ALL")
 Description: Creates a set of buffers for the set of input features. The buffers
              are defined using a set of variable distances. The resulting feature
              class has the merged buffer polygons with or without overlapping 
              polygons maintained as seperate features.
----------------------------------------------------------------------------------'''

import arcgisscripting
import os
import sys
import types
import arcpy
from arcpy import env



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
else: #in this case, the check box value is 'false', user did not check the box
	space = mdb_loc + '/' + mdb_name
	arcpy.env.workspace = space
	arcpy.AddMessage('Use existing database')
#buffer_dist = arcpy.GetParameter(5)


request_folder = request_id
loc_out = request_id + "_LOC"
loc_table = 'loc_table'
#loc_file = loc_name + '.xls\SHEET1$'
#loc_import = home_path + '/' + request_folder + '/' + loc_file
buffer_name = request_id + "_Buffer"
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





#buffer
#comment out if using multi-ring buffer tool
#arcpy.Buffer_analysis(loc_out,buffer_name,str(buffer_dist) + " miles","FULL","ROUND","NONE","#")

#############################################

gp = arcgisscripting.create(9.3)

#Define message constants so they may be translated easily
msgBuffRings  = gp.GetIDMessage(86149) #"Buffering distance "
msgMergeRings = gp.GetIDMessage(86150) #"Merging rings..."
msgDissolve   = gp.GetIDMessage(86151) #"Dissolving overlapping boundaries..."

buffer_in = mdb_loc + '/' + mdb_name + '/' + loc_out
buffer_out = mdb_loc + '/' + mdb_name + '/' + buffer_name

def initiateMultiBuffer():
    

    # Get the input argument values
    # Input FC
    input           = buffer_in
    # Output FC
    output          = buffer_out
    # Distances
    distances       = gp.GetParameter(5)
    # Unit
    unit            = gp.GetParameterAsText(6)
    if unit.lower() == "default":
        unit = ""
    # If no field name is specified, use the name "distance" by default
    fieldName       = checkFieldName(gp, gp.GetParameterAsText(7), os.path.dirname(output))
    #Dissolve option    
    dissolveOption  = gp.GetParameterAsText(8)
    # Outside Polygons    
    outsidePolygons = gp.GetParameterAsText(9)
    if outsidePolygons.lower() == "true":
        sideType = "OUTSIDE_ONLY"
    else:
        sideType = ""
         
    createMultiBuffers(gp, input, output, distances, unit, fieldName, dissolveOption, sideType)


def checkFieldName(gp, fieldName, workspace):
    if fieldName == "#" or fieldName == '':
        return "distance"
    else:
        if len(fieldName) > 10:
            outName = gp.ValidateFieldName(fieldName, workspace)
            if outName != fieldName:
                gp.AddIDMessage("WARNING", 648, outName)
            return outName
        else:
            newName = gp.ValidateFieldName(fieldName, workspace)
            if newName != fieldName:
                gp.AddIDMessage("WARNING", 648, newName)
            return newName

def convertValueTableToList(valTable):
    outList = [float(v) for v in valTable.exporttostring().split(";")]
    return outList

def lowerLicenseUnion(gp, fcList):
    unionFC = None
    tmpFC = gp.Union_analysis(fcList[0:2],
                              gp.CreateUniqueName("union", scratchWks)).getOutput(0)
    for fc in fcList[2:]:
        if unionFC:
            tmpFC = unionFC
        unionFC = gp.Union_analysis([tmpFC, fc],
                                    gp.CreateUniqueName("union", scratchWks)).getOutput(0)
    return unionFC
    

def createMultiBuffers(gp, input, output, distances, unit, fieldName, dissolveOption, sideType):
    try:
        global scratchWks
        # Assign empty values to aid with cleanup at the end
        #        
        oldOW, cleanupFlag = None, None

        # Keep track of current settings that should be restored by end
        if not gp.overwriteOutput:
            oldOW = True
            gp.overwriteOutput = True

        if gp.Exists(os.path.join(str(gp.scratchWorkspace), "scratch.gdb")):
            scratchWks = os.path.join(gp.scratchWorkspace, "scratch.gdb")
        else:
            cleanupFlag = True
            scratchWks = gp.CreateScratchName("xxx", "mrbuffgdb.gdb", "",
                                              gp.GetSystemEnvironment("TEMP"))
            gp.CreateFileGDB_management(*os.path.split(scratchWks))

        # Convert the distances into a Python list for ease of use
        distList = convertValueTableToList(distances)

        # Loop through each distance creating a new layer and then buffering the input.
        #  Set the step progressor if there are > 1 rings
        if len(distList) > 1:
            gp.SetProgressor("step", "", 0, len(distList))
            stepProg = True
        else:
            gp.SetProgressor("default")
            stepProg = False
       
        bufferedList = []

        # Buffer the input for each buffer distance.  If the fieldName is different than
        #  the default, add a new field and calculate the proper value
        for dist in distList:
            if stepProg:
                gp.SetProgressorPosition()
            gp.SetProgressorLabel(msgBuffRings + str(dist) + "...")
            bufDistance = "%s %s" % (dist, unit)
            bufOutput = gp.Buffer_analysis(input, gp.CreateUniqueName("buffer", scratchWks),
                                           bufDistance, sideType, "", dissolveOption).getOutput(0)
            if fieldName.lower() != "buff_dist":
                gp.AddField_management(bufOutput, fieldName, "double")
                gp.CalculateField_management(bufOutput, fieldName, dist, "PYTHON")
            bufferedList.append(bufOutput)

        gp.ResetProgressor()
        gp.SetProgressor("default")
        gp.SetProgressorLabel(msgMergeRings)

        if dissolveOption == "ALL":
            # Set up the expression and codeblock variables for CalculateField to ensure
            #  the distance field is populated properly
            expression = "pullDistance(" + str(distList) + ", "
            for fc in bufferedList:
                expression += "!FID_" + os.path.basename(fc) +  "!, "
            expression = expression[:-2] + ")"
            
            # If we have a full license then Union all feature classes at once, otherwise
            #  Union the feature classes 2 at a time
            if gp.ProductInfo().upper() in ["ARCINFO", "ARCSERVER"] or len(bufferedList) < 3:
                unionFC = gp.Union_analysis(bufferedList,
                                            gp.CreateUniqueName("union", scratchWks)).getOutput(0)
                codeblock = '''def pullDistance(distL, *fids):
                return min([i for i, j in zip(distL, fids) if j != -1])'''            
            else:
                unionFC = lowerLicenseUnion(gp, bufferedList)
                codeblock = '''def pullDistance(distL, *fids):
                return min([i for i, j in zip(distL, fids) if j == 1])'''

            gp.CalculateField_management(unionFC, fieldName, expression, "PYTHON", codeblock)

            # Complete the final Dissolve
            gp.SetProgressorLabel(msgDissolve)
            if dissolveOption.upper() == "ALL":
                gp.Dissolve_management(unionFC, output, fieldName)
        else:
            # Reverse the order of the inputs so the features are appended from
            #  largest to smallest buffer features.
            bufferedList.reverse()
            template = bufferedList[0]
            if gp.OutputCoordinateSystem:
                sr = gp.OutputCoordinateSystem
            else:
                sr = gp.Describe(template).spatialreference
            gp.CreateFeatureClass_management(os.path.dirname(output), os.path.basename(output),
                                             "POLYGON", template, "SAME_AS_TEMPLATE", "SAME_AS_TEMPLATE", sr)
            for fc in bufferedList:
                gp.Append_management(fc, output)

        # Set the default symbology
        params = gp.GetParameterInfo()
        if len(params) > 0:
            params[1].symbology = os.path.join(gp.GetInstallInfo()['InstallDir'],
                                               "arctoolbox\\templates\\layers\\multipleringbuffer.lyr")

    except arcgisscripting.ExecuteError:
        gp.AddError(gp.GetMessages(2))

    except Exception as err:
        gp.AddError(err.message)

    finally:
        # Cleanup tasks
        if oldOW:
            gp.OverWriteOutput = False
        if cleanupFlag:
            try:
                gp.Delete_management(scrGDB)
            except:
                pass

if __name__ == '__main__':
    initiateMultiBuffer()
	
#############################################






layer = arcpy.mapping.Layer(buffer_name)
arcpy.mapping.AddLayer(df,layer)

layer2 = arcpy.mapping.Layer(loc_out)
arcpy.mapping.AddLayer(df,layer2)

arcpy.RefreshTOC()
arcpy.RefreshActiveView()


#select and print states that intersect the buffer
arcpy.SelectLayerByLocation_management("Detailed\Geo Boundaries\States","INTERSECT",buffer_name)
arcpy.AddMessage('Create Spatial View for these States:')
for row in arcpy.SearchCursor("Detailed\Geo Boundaries\States"):
	states = row.NAME
	arcpy.AddMessage(states)

mxd.save()
