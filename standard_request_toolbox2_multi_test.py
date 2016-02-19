import arcpy
from arcpy import env
from arcpy import mapping 
import time  
import pypyodbc
import csv


arcpy.AddMessage('\nDefining Variables')
time1 = time.clock()
#define input parameters
request_in = arcpy.GetParameterAsText(0)
request_id = request_in.upper()
# mdb_path = arcpy.GetParameterAsText(1)
# arcpy.env.workspace = mdb_path
arcpy.env.overwriteOutput = True
#define optional layers to visible
zip = arcpy.GetParameterAsText(1)
if str(zip) == 'true':
	zip_on = 'yes'
else:
	zip_on = 'no'

split = arcpy.GetParameterAsText(2)
if str(split) == 'true':
	split_on = 'yes'
else:
	split_on = 'no'

route = arcpy.GetParameterAsText(3)
if str(route) == 'true':
	route_on = 'yes'
else:
	route_on = 'no'

major_road = arcpy.GetParameterAsText(4)
if str(major_road) == 'true':
	major_road_on = 'Major Road'
else:
	major_road_on = 'no'

residental_street = arcpy.GetParameterAsText(5)
if str(residental_street) == 'true':
	residental_street_on = 'Residental Street'
else:
	residental_street_on = 'no'

client_name = arcpy.GetParameterAsText(6)
city_state = arcpy.GetParameterAsText(7)	

report_variables = arcpy.GetParameterAsText(8)

map_path = arcpy.GetParameterAsText(9)


	
#define standard and optional visible layers	
optional_on = [major_road_on,residental_street_on]

standard_on = ['Cities & Towns','All Towns','Street Network','Interstate','Highway',	
'Geo Boundaries','States','Shared Mail Boundaries','Zipcode','Water','Base Layers',	
'Backgrounds','US Background','Water','Legend','Locations','Location','Buffer','Geography','study_area']
	

#define request specific variable names	
loc_name = request_id + '_LOC'
buffer_name = request_id + '_Buffer'
zip_view = 'MAPPING.SDEADMIN.SVWZ_' + request_id 
split_view = 'MAPPING.SDEADMIN.SVWS_' + request_id
route_view = 'MAPPING.SDEADMIN.SVWR_' + request_id


#define layer files to import symbology from
zip_lyr = r'H:\PROJECTS\SALES REPS\BRIAN HANKINS\requests\R64609790\lyr\zip_layer.lyr'
split_lyr = r'H:\PROJECTS\SALES REPS\BRIAN HANKINS\requests\R64609790\lyr\split_layer.lyr'
route_lyr = r'H:\PROJECTS\SALES REPS\BRIAN HANKINS\requests\R64609790\lyr\route_layer.lyr'
zip_base = r'H:\PROJECTS\SALES REPS\BRIAN HANKINS\requests\R64609790\lyr\zip_base_gray_30.lyr'
location_lyr =  r'H:\PROJECTS\SALES REPS\BRIAN HANKINS\requests\R64609790\lyr\location.lyr'
buffer_lyr = r'H:\PROJECTS\SALES REPS\BRIAN HANKINS\requests\R64609790\lyr\buffer.lyr'


mxd = arcpy.mapping.MapDocument(r"Current")
view_lyr = arcpy.mapping.ListLayers(mxd)
df = arcpy.mapping.ListDataFrames(mxd,"Layers")[0]
time2 = time.clock()  
arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")


arcpy.AddMessage('\nApply Buffer and Location Symbology')
time1 = time.clock()

arcpy.ApplySymbologyFromLayer_management(buffer_name,buffer_lyr)
for layer in view_lyr:
	if layer.name == loc_name:
		sourcelayer = arcpy.mapping.Layer(r'H:\PROJECTS\SALES REPS\BRIAN HANKINS\requests\R64609790\lyr\location.lyr')
		arcpy.mapping.UpdateLayer(df,layer,sourcelayer, True)
		layer.showLabels = True
time2 = time.clock()  
arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")

arcpy.AddMessage('\nApply Map Title')
time1 = time.clock()
for text1 in arcpy.mapping.ListLayoutElements(mxd,"TEXT_ELEMENT"):
	if text1.text == 'STORE':
		text1.text = client_name
	if text1.text != client_name:
		text1.text = '<dyn type="page" property="name"/>'
time2 = time.clock()  
arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")


#Generate Reports
arcpy.AddMessage('\nGenerate Reports')
time1 = time.clock()
connection_string = 'DRIVER={SQL Server};SERVER=MS-SQL2;DATABASE=mapping;UID=sdeadmin;PWD=theo'
con = pypyodbc.connect(connection_string)

report_layers = arcpy.mapping.ListLayers(mxd)
for lyr in report_layers:
	if lyr.name == zip_view:
		variables = report_variables
		geo = 'zip'
		#con = pypyodbc.connect('DRIVER={SQL Server};SERVER=MS-SQL2;DATABASE=mapping;UID=sdeadmin;PWD=theo')
		sql_command = 'EXEC [sdeadmin].standardreport ' + "'tblz_" + request_in + "'" + ', ' +"'" + variables +"'" + ', ' + "'" + geo +"'"
		cur = con.cursor().execute(sql_command)
		with open('zip_report.csv','wb') as csv_file:
			csv_writer = csv.writer(csv_file)
			csv_writer.writerow([i[0] for i in cur.description])
			csv_writer.writerows(cur)
		#con.close()

for lyr in report_layers:
	if lyr.name == split_view:
		variables = report_variables
		geo = 'zip_split'
		#con = pypyodbc.connect('DRIVER={SQL Server};SERVER=MS-SQL2;DATABASE=mapping;UID=sdeadmin;PWD=theo')
		sql_command = 'EXEC [sdeadmin].standardreport ' + "'tbls_" + request_in + "'" + ', ' +"'" + variables +"'" + ', ' + "'" + geo +"'"
		cur = con.cursor()
		cur.execute(sql_command)
		with open('split_report.csv','wb') as csv_file:
			csv_writer = csv.writer(csv_file)
			csv_writer.writerow([i[0] for i in cur.description])
			csv_writer.writerows(cur)
		#con.close()

for lyr in report_layers:
	if lyr.name == route_view:
		variables = report_variables
		geo = 'cr_id'
		#con = pypyodbc.connect('DRIVER={SQL Server};SERVER=MS-SQL2;DATABASE=mapping;UID=sdeadmin;PWD=theo')
		sql_command = 'EXEC [sdeadmin].standardreport ' + "'tblr_" + request_in + "'" + ', ' +"'" + variables +"'" + ', ' + "'" + geo +"'"
		cur = con.cursor()
		cur.execute(sql_command)
		with open('route_report.csv','wb') as csv_file:
			csv_writer = csv.writer(csv_file)
			csv_writer.writerow([i[0] for i in cur.description])
			csv_writer.writerows(cur)
		#con.close()

		
con.close()
		
time2 = time.clock()  
arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")


mxd.save()

#Generate Zip map		
if zip_on == 'yes':
	time1 = time.clock() 
	arcpy.AddMessage('\nGenerating Zip Map')
	arcpy.AddMessage('\nApply Spatial Geography')
	# for maplayer in view_lyr:
		# if maplayer.name == zip_view:
			# arcpy.ApplySymbologyFromLayer_management(zip_view,zip_lyr)
	time2 = time.clock()  
	arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")

	
	
	arcpy.AddMessage('\nMoving Layers')
	# move layers
	time1 = time.clock()
	layer_list = arcpy.mapping.ListLayers(mxd)
	for y in layer_list:
		if y.name == 'Available Zips':
			move_lyr = arcpy.mapping.Layer('Available Zips')
	refLayer = move_lyr
	for lyr in layer_list:
		if lyr.name == zip_view:
			zip_move = lyr
	for lyr in layer_list:		
		if lyr.name == zip_view:
			arcpy.mapping.MoveLayer(df, refLayer, zip_move, "BEFORE")
	time2 = time.clock()  
	arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")
	
	
	arcpy.AddMessage('\nTurn on visible layers')
	#layer management
	time1 = time.clock()
	names = [buffer_name,loc_name,'zip_legend']
	layers = arcpy.mapping.ListLayers(mxd, "*", df)
	for layer in layers:
		layer.visible = False
		if layer.name in standard_on:
			layer.visible = True
		if layer.name in optional_on:
			layer.visible = True
		if layer.name in names:
			layer.visible = True
	time2 = time.clock()  
	arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")
	

	
	arcpy.AddMessage('\nTurn on Detailed Layer')
	#turn on Detail layers
	layers = arcpy.mapping.ListLayers(mxd)
	for layer in layers:
		if layer.name == 'Detailed':
			layer.visible = True
	time2 = time.clock()  
	arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")
	
	
	
	arcpy.AddMessage('\nRefresh and redraw active view')
	time1 = time.clock()
	arcpy.RefreshTOC()
	arcpy.RefreshActiveView()
	time2 = time.clock()  
	arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")
	
	
	
# Determing total number of features in view feature class
	arcpy.AddMessage('Counting Features in View')
	count = int(arcpy.GetCount_management(zip_view).getOutput(0))
	if count > 500:
	
		
		arcpy.AddMessage('More than 500 features')
		#export DDP to multiple files
		time1 = time.clock()
		mxd = arcpy.mapping.MapDocument(r"Current")
		lyr = arcpy.mapping.ListLayers(mxd)
		map_name = request_id.upper() + '_' + client_name + '_Zip Map.pdf'
		export_loc = map_path + '/' + map_name
		for layers in lyr:
			if layers.name == buffer_name:
				buffer_ddp = layers
		for pageNum in range(1, mxd.dataDrivenPages.pageCount + 1):
			mxd.dataDrivenPages.currentPageID = pageNum
			storeName = mxd.dataDrivenPages.pageRow.store  #Get field used in dynamic query     
			buffer_ddp.definitionQuery = 'store = ' + "'" + storeName + "'"  #apply dynamic query     
			# Extent Polygon 
			# Select geography from view based on current extent of map.  This is to eliminate the issues of applyung symbology when there are more than 500 features in view feature class.
			mxd2 = arcpy.mapping.MapDocument(r"CURRENT")
			df2 = arcpy.mapping.ListDataFrames(mxd2)[0]
			lyr2 = arcpy.mapping.ListLayers(mxd2, "*SVWS*", df2)
			#get current extent
			extentPolygon = arcpy.Polygon(arcpy.Array([df2.extent.lowerLeft,df2.extent.lowerRight, df2.extent.upperRight, df2.extent.upperLeft]),df2.spatialReference)
			# Select all features in current extent
			for lyr in lyr2:
				if lyr.name == zip_view:
					arcpy.SelectLayerByLocation_management(lyr, "INTERSECT", extentPolygon, "", "NEW_SELECTION")
			
			zip_layer_ddp = 'zip_layer_ddp'
			
			# Copy features from view into temporary feature class
			# Add layer and apply symbology
			arcpy.CopyFeatures_management(zip_view,zip_layer_ddp)
			layer_add = arcpy.mapping.Layer(zip_layer_ddp)
			arcpy.mapping.AddLayer(df2,layer_add)
			arcpy.ApplySymbologyFromLayer_management(zip_layer_ddp,zip_lyr)
			
			# Move layer copy to correct position
			mxd3 = arcpy.mapping.MapDocument(r"Current")
			lyr_move = arcpy.mapping.ListLayers(mxd3)
			zip_ddp_move = 'zip_layer_ddp'
			for layer in lyr_move:
				if layer.name == zip_view:
					ref_lyr = layer
			refLayer = ref_lyr
			for layer in lyr_move:
				if layer.name == zip_layer_ddp:
					move_lyr = layer
			for layer in lyr_move:
				if layer.name == zip_layer_ddp:
					arcpy.mapping.MoveLayer(df, refLayer, move_lyr, "BEFORE")
			
			arcpy.RefreshActiveView()
			# export pdf map
			arcpy.AddMessage("Exporting page {0} of {1}".format(str(mxd.dataDrivenPages.currentPageID), str(mxd.dataDrivenPages.pageCount)))
			tmpPdf = export_loc 
			ddp = mxd.dataDrivenPages
			ddp.exportToPDF(tmpPdf,"CURRENT",'',"PDF_MULTIPLE_FILES_PAGE_NAME")	
			# delete copy layer
			arcpy.Delete_management(zip_layer_ddp)
		
		buffer_ddp.definitionQuery = ''
		time2 = time.clock()  
		arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")

	else:
		arcpy.AddMessage('Less than 500 features')
		#export DDP to multiple files
		time1 = time.clock()
		
		# Add layer and apply symbology
		arcpy.ApplySymbologyFromLayer_management(zip_view,zip_lyr)
		
		# Move layer copy to correct position
		mxd = arcpy.mapping.MapDocument(r"Current")
		lyr_move = arcpy.mapping.ListLayers(mxd)
		# zip_move = zip_view
		# for layer in lyr_move:
			# if layer.name == zip_view:
				# ref_lyr = layer
		# refLayer = ref_lyr
		for layer in lyr_move:
			if layer.name == zip_view:
				# move_lyr = layer
				layer.visible = True
		# for layer in lyr_move:
			# if layer.name == zip_view:
				# arcpy.mapping.MoveLayer(df, refLayer, move_lyr, "BEFORE")
				
		lyr = arcpy.mapping.ListLayers(mxd)
		map_name = request_id.upper() + '_' + client_name + '_Zip Map.pdf'
		export_loc = map_path + '/' + map_name
		for layers in lyr:
			if layers.name == buffer_name:
				buffer_ddp = layers
		for pageNum in range(1, mxd.dataDrivenPages.pageCount + 1):
			mxd.dataDrivenPages.currentPageID = pageNum
			storeName = mxd.dataDrivenPages.pageRow.store  #Get field used in dynamic query     
			buffer_ddp.definitionQuery = 'store = ' + "'" + storeName + "'"  #apply dynamic query     
			arcpy.RefreshActiveView()
			arcpy.AddMessage("Exporting page {0} of {1}".format(str(mxd.dataDrivenPages.currentPageID), str(mxd.dataDrivenPages.pageCount)))
			tmpPdf = export_loc 
			ddp = mxd.dataDrivenPages
			ddp.exportToPDF(tmpPdf,"CURRENT",'',"PDF_MULTIPLE_FILES_PAGE_NAME")	
		buffer_ddp.definitionQuery = ''
		time2 = time.clock()  
		arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")
	
	
#Generate zip split map
if split_on == 'yes':
	time1 = time.clock() 
	arcpy.AddMessage('\nGenerate Split Map')
	arcpy.AddMessage('\nApply Spatial Geography')	
	for maplayer in view_lyr:
		if maplayer.name == split_view:
			dq1 = 'zip'
			dq2 = 'zip_split'
			queryStr = dq1 + '<>' + dq2
			maplayer.definitionQuery = queryStr
			# arcpy.ApplySymbologyFromLayer_management(split_view,split_lyr)
		if maplayer.name == zip_view:
			arcpy.ApplySymbologyFromLayer_management(zip_view,zip_base)
		if maplayer.name == 'Split':
			dqst1 = 'SHAPE.area >.000005 AND zip <> tpz AND tpz IN (SELECT zip_split FROM tbls_' + request_id + ')'
			maplayer.definitionQuery = dqst1
	time2 = time.clock()  
	arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")
	
	
	# move layers
	time1 = time.clock() 
	arcpy.AddMessage('\nMoving Layers')
	layer_list = arcpy.mapping.ListLayers(mxd)
	for y in layer_list:
		if y.name == 'Available Zips':
			move_lyr = arcpy.mapping.Layer('Available Zips')
	refLayer = move_lyr
	for lyr in layer_list:
		if lyr.name == zip_view:
			zip_move = lyr
		if lyr.name == split_view:
			split_move = lyr
	for lyr in layer_list:
		if lyr.name == split_view:
			arcpy.mapping.MoveLayer(df, refLayer, split_move, "BEFORE")
	for lyr in layer_list:	
		if lyr.name == zip_view:
			arcpy.mapping.MoveLayer(df, refLayer, zip_move, "BEFORE")
	time2 = time.clock()  
	arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")


	time1 = time.clock() 
	arcpy.AddMessage('\nTurn on visible layers')
	# layer management
	names = [zip_view,buffer_name,loc_name]
	layers = arcpy.mapping.ListLayers(mxd, "*", df)
	for layer in layers:
		layer.visible = False
		if layer.name in standard_on:
			layer.visible = True
		if layer.name in optional_on:
			layer.visible = True
		if layer.name in names:
			layer.visible = True
		if layer.name == 'split_legend':
			layer.visible = True
		if layer.name == 'Split':
			layer.visible = True
		if layer.name == 'zip_no_split':
			layer.visible = True
	time2 = time.clock()  
	arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")		
	
	

	
	time1 = time.clock() 
	arcpy.AddMessage('\nTurn on Detailed Layer')
	#turn on Detail layers
	layers = arcpy.mapping.ListLayers(mxd)
	for layer in layers:
		if layer.name == 'Detailed':
			layer.visible = True
	time2 = time.clock()  
	arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")
	
	
	time1 = time.clock() 
	arcpy.AddMessage('\nRefresh and redraw active view')
	arcpy.RefreshTOC()
	arcpy.RefreshActiveView()
	time2 = time.clock()  
	arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")

	
# Determing total number of features in view feature class
	arcpy.AddMessage('Counting Features in View')
	count = int(arcpy.GetCount_management(split_view).getOutput(0))
	
	if count > 500:
	
		
		arcpy.AddMessage('More than 500 features')
		#export DDP to multiple files
		time1 = time.clock()
		mxd = arcpy.mapping.MapDocument(r"Current")
		lyr = arcpy.mapping.ListLayers(mxd)
		map_name = request_id.upper() + '_' + client_name + '_ZipSplit Map.pdf'
		export_loc = map_path + '/' + map_name
		for layers in lyr:
			if layers.name == buffer_name:
				buffer_ddp = layers
		for pageNum in range(1, mxd.dataDrivenPages.pageCount + 1):
			mxd.dataDrivenPages.currentPageID = pageNum
			storeName = mxd.dataDrivenPages.pageRow.store  #Get field used in dynamic query     
			buffer_ddp.definitionQuery = 'store = ' + "'" + storeName + "'"  #apply dynamic query     
			# Extent Polygon 
			# Select geography from view based on current extent of map.  This is to eliminate the issues of applyung symbology when there are more than 500 features in view feature class.
			mxd2 = arcpy.mapping.MapDocument(r"CURRENT")
			df2 = arcpy.mapping.ListDataFrames(mxd2)[0]
			lyr2 = arcpy.mapping.ListLayers(mxd2, "*SVWS*", df2)
			#get current extent
			extentPolygon = arcpy.Polygon(arcpy.Array([df2.extent.lowerLeft,df2.extent.lowerRight, df2.extent.upperRight, df2.extent.upperLeft]),df2.spatialReference)
			# Select all features in current extent
			for lyr in lyr2:
				if lyr.name == split_view:
					arcpy.SelectLayerByLocation_management(lyr, "INTERSECT", extentPolygon, "", "NEW_SELECTION")
			
			split_layer_ddp = 'split_layer_ddp'
			
			# Copy features from view into temporary feature class
			# Add layer and apply symbology
			arcpy.CopyFeatures_management(split_view,split_layer_ddp)
			layer_add = arcpy.mapping.Layer(split_layer_ddp)
			arcpy.mapping.AddLayer(df2,layer_add)
			arcpy.ApplySymbologyFromLayer_management(split_layer_ddp,split_lyr)
			
			# Move layer copy to correct position
			mxd3 = arcpy.mapping.MapDocument(r"Current")
			lyr_move = arcpy.mapping.ListLayers(mxd3)
			split_ddp_move = 'split_layer_ddp'
			for layer in lyr_move:
				if layer.name == zip_view:
					ref_lyr = layer
			refLayer = ref_lyr
			for layer in lyr_move:
				if layer.name == split_layer_ddp:
					move_lyr = layer
			for layer in lyr_move:
				if layer.name == split_layer_ddp:
					arcpy.mapping.MoveLayer(df, refLayer, move_lyr, "BEFORE")
			
			arcpy.RefreshActiveView()
			# export pdf map
			arcpy.AddMessage("Exporting page {0} of {1}".format(str(mxd.dataDrivenPages.currentPageID), str(mxd.dataDrivenPages.pageCount)))
			tmpPdf = export_loc 
			ddp = mxd.dataDrivenPages
			ddp.exportToPDF(tmpPdf,"CURRENT",'',"PDF_MULTIPLE_FILES_PAGE_NAME")	
			# delete copy layer
			arcpy.Delete_management(split_layer_ddp)
		
		buffer_ddp.definitionQuery = ''
		time2 = time.clock()  
		arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")

	else:
		arcpy.AddMessage('Less than 500 features')
		#export DDP to multiple files
		time1 = time.clock()
		
		# Add layer and apply symbology
		arcpy.ApplySymbologyFromLayer_management(split_view,split_lyr)
		
		# Move layer copy to correct position
		mxd = arcpy.mapping.MapDocument(r"Current")
		lyr_move = arcpy.mapping.ListLayers(mxd)
		split_move = split_view
		# for layer in lyr_move:
			# if layer.name == zip_view:
				# ref_lyr = layer
		# refLayer = ref_lyr
		for layer in lyr_move:
			if layer.name == split_view:
				# move_lyr = layer
				layer.visible = True
		# for layer in lyr_move:
			# if layer.name == split_view:
				# arcpy.mapping.MoveLayer(df, refLayer, move_lyr, "BEFORE")
				
		lyr = arcpy.mapping.ListLayers(mxd)
		map_name = request_id.upper() + '_' + client_name + '_ZipSplit Map.pdf'
		export_loc = map_path + '/' + map_name
		for layers in lyr:
			if layers.name == buffer_name:
				buffer_ddp = layers
		for pageNum in range(1, mxd.dataDrivenPages.pageCount + 1):
			mxd.dataDrivenPages.currentPageID = pageNum
			storeName = mxd.dataDrivenPages.pageRow.store  #Get field used in dynamic query     
			buffer_ddp.definitionQuery = 'store = ' + "'" + storeName + "'"  #apply dynamic query     
			arcpy.RefreshActiveView()
			arcpy.AddMessage("Exporting page {0} of {1}".format(str(mxd.dataDrivenPages.currentPageID), str(mxd.dataDrivenPages.pageCount)))
			tmpPdf = export_loc 
			ddp = mxd.dataDrivenPages
			ddp.exportToPDF(tmpPdf,"CURRENT",'',"PDF_MULTIPLE_FILES_PAGE_NAME")	
		buffer_ddp.definitionQuery = ''
		time2 = time.clock()  
		arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")


	
# Generate Route map		
if route_on == 'yes':
	time1 = time.clock() 
	arcpy.AddMessage('\nGenerate Route Map')
	arcpy.AddMessage('\nApply Spatial Geography')
	for maplayer in view_lyr:
		# if maplayer.name == route_view:
			# arcpy.ApplySymbologyFromLayer_management(route_view,route_lyr)
		if maplayer.name == zip_view:
				arcpy.ApplySymbologyFromLayer_management(zip_view,zip_base)	
		if maplayer.name == 'Route':
			dqst2 = 'SHAPE.area >.000005 AND cr_id IN (SELECT cr_id FROM tblr_' + request_id + ')'
			maplayer.definitionQuery = dqst2
	time2 = time.clock()  
	arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")
	
	
	# move layers
	time1 = time.clock() 
	arcpy.AddMessage('\nMoving Layers')
	layer_list = arcpy.mapping.ListLayers(mxd)
	for y in layer_list:
		if y.name == 'Available Zips':
			move_lyr = arcpy.mapping.Layer('Available Zips')
	refLayer = move_lyr
	for lyr in layer_list:
		if lyr.name == zip_view:
			zip_move = lyr
		if lyr.name == route_view:
			route_move = lyr
	for lyr in layer_list:	
		if lyr.name == route_view:
			arcpy.mapping.MoveLayer(df, refLayer, route_move, "BEFORE")
	for lyr in layer_list:
		if lyr.name == zip_view:
			arcpy.mapping.MoveLayer(df, refLayer, zip_move, "BEFORE")
	time2 = time.clock()  
	arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")			

	
	
	time1 = time.clock() 
	arcpy.AddMessage('\nTurn on visible layers')		
	# layer management - turn on correct layers
	names = [zip_view,buffer_name,loc_name]
	layers = arcpy.mapping.ListLayers(mxd, "*", df)
	for layer in layers:
		layer.visible = False
		if layer.name in standard_on:
			layer.visible = True
		if layer.name in optional_on:
			layer.visible = True
		if layer.name in names:
			layer.visible = True
		if layer.name == 'Route':
			layer.visible = True	
		if layer.name == 'route_legend':
			layer.visible = True
		if layer.name == 'zip_no_route':
			layer.visible = True
	time2 = time.clock()  
	arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")


	
	time1 = time.clock() 
	arcpy.AddMessage('\nTurn on Detailed Layer')
	#turn on Detail layers
	layers = arcpy.mapping.ListLayers(mxd)
	for layer in layers:
		if layer.name == 'Detailed':
			layer.visible = True
	time2 = time.clock()  
	arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")
	
			
			
	time1 = time.clock() 
	arcpy.AddMessage('\nRefresh and redraw active view')
	arcpy.RefreshTOC()
	arcpy.RefreshActiveView()
	time2 = time.clock()  
	arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")
	
	# Determing total number of features in view feature class
	arcpy.AddMessage('Counting Features in View')
	count = int(arcpy.GetCount_management(route_view).getOutput(0))
	
	if count > 500:
	
		
		arcpy.AddMessage('More than 500 features')
		#export DDP to multiple files
		time1 = time.clock()
		mxd = arcpy.mapping.MapDocument(r"Current")
		lyr = arcpy.mapping.ListLayers(mxd)
		map_name = request_id.upper() + '_' + client_name + '_Route Map.pdf'
		export_loc = map_path + '/' + map_name
		for layers in lyr:
			if layers.name == buffer_name:
				buffer_ddp = layers
		for pageNum in range(1, mxd.dataDrivenPages.pageCount + 1):
			mxd.dataDrivenPages.currentPageID = pageNum
			storeName = mxd.dataDrivenPages.pageRow.store  #Get field used in dynamic query     
			buffer_ddp.definitionQuery = 'store = ' + "'" + storeName + "'"  #apply dynamic query     
			# Extent Polygon 
			# Select geography from view based on current extent of map.  This is to eliminate the issues of applyung symbology when there are more than 500 features in view feature class.
			mxd2 = arcpy.mapping.MapDocument(r"CURRENT")
			df2 = arcpy.mapping.ListDataFrames(mxd2)[0]
			lyr2 = arcpy.mapping.ListLayers(mxd2, "*SVWR*", df2)
			#get current extent
			extentPolygon = arcpy.Polygon(arcpy.Array([df2.extent.lowerLeft,df2.extent.lowerRight, df2.extent.upperRight, df2.extent.upperLeft]),df2.spatialReference)
			# Select all features in current extent
			for lyr in lyr2:
				if lyr.name == route_view:
					arcpy.SelectLayerByLocation_management(lyr, "INTERSECT", extentPolygon, "", "NEW_SELECTION")
				# if lyr.name == 'route_layer_ddp':
			route_layer_ddp = 'route_layer_ddp'
			
			# Copy features from view into temporary feature class
			# Add layer and apply symbology
			arcpy.CopyFeatures_management(route_view,route_layer_ddp)
			layer_add = arcpy.mapping.Layer(route_layer_ddp)
			arcpy.mapping.AddLayer(df2,layer_add)
			arcpy.ApplySymbologyFromLayer_management(route_layer_ddp,route_lyr)
			
			# Move layer copy to correct position
			mxd3 = arcpy.mapping.MapDocument(r"Current")
			lyr_move = arcpy.mapping.ListLayers(mxd3)
			route_ddp_move = 'route_layer_ddp'
			for layer in lyr_move:
				if layer.name == zip_view:
					ref_lyr = layer
			refLayer = ref_lyr
			for layer in lyr_move:
				if layer.name == route_layer_ddp:
					move_lyr = layer
			for layer in lyr_move:
				if layer.name == route_layer_ddp:
					arcpy.mapping.MoveLayer(df, refLayer, move_lyr, "BEFORE")
			
			arcpy.RefreshActiveView()
			# export pdf map
			arcpy.AddMessage("Exporting page {0} of {1}".format(str(mxd.dataDrivenPages.currentPageID), str(mxd.dataDrivenPages.pageCount)))
			tmpPdf = export_loc 
			ddp = mxd.dataDrivenPages
			ddp.exportToPDF(tmpPdf,"CURRENT",'',"PDF_MULTIPLE_FILES_PAGE_NAME")	
			# delete copy layer
			arcpy.Delete_management(route_layer_ddp)
		
		buffer_ddp.definitionQuery = ''
		time2 = time.clock()  
		arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")

	else:
		arcpy.AddMessage('Less than 500 features')
		#export DDP to multiple files
		time1 = time.clock()
		
		# Add layer and apply symbology
		arcpy.ApplySymbologyFromLayer_management(route_view,route_lyr)
		
		# Move layer copy to correct position
		mxd = arcpy.mapping.MapDocument(r"Current")
		lyr_move = arcpy.mapping.ListLayers(mxd)
		route_move = route_view
		# for layer in lyr_move:
			# if layer.name == zip_view:
				# ref_lyr = layer
		# refLayer = ref_lyr
		for layer in lyr_move:
			if layer.name == route_view:
				# move_lyr = layer
				layer.visible = True
		# for layer in lyr_move:
			# if layer.name == route_view:
				# arcpy.mapping.MoveLayer(df, refLayer, move_lyr, "BEFORE")
				
		lyr = arcpy.mapping.ListLayers(mxd)
		map_name = request_id.upper() + '_' + client_name + '_Route Map.pdf'
		export_loc = map_path + '/' + map_name
		for layers in lyr:
			if layers.name == buffer_name:
				buffer_ddp = layers
		for pageNum in range(1, mxd.dataDrivenPages.pageCount + 1):
			mxd.dataDrivenPages.currentPageID = pageNum
			storeName = mxd.dataDrivenPages.pageRow.store  #Get field used in dynamic query     
			buffer_ddp.definitionQuery = 'store = ' + "'" + storeName + "'"  #apply dynamic query     
			arcpy.RefreshActiveView()
			arcpy.AddMessage("Exporting page {0} of {1}".format(str(mxd.dataDrivenPages.currentPageID), str(mxd.dataDrivenPages.pageCount)))
			tmpPdf = export_loc 
			ddp = mxd.dataDrivenPages
			ddp.exportToPDF(tmpPdf,"CURRENT",'',"PDF_MULTIPLE_FILES_PAGE_NAME")	
		buffer_ddp.definitionQuery = ''
		time2 = time.clock()  
		arcpy.AddMessage("Processing Time: " + str(time2-time1) + " seconds")



