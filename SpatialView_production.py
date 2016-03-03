
import arcpy
# from arcpy import env
# from arcpy import mapping 
import time  
# import pypyodbc
import msparkdb

#define input parameters
# spatial view prefix
table_name_in = arcpy.GetParameterAsText(0)
table_name = table_name_in.upper()

# define level(s) of geography to create views for:
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

# run clean spatial view stored procedure.  
# will run on all views created if ths is selected.  
# default is yes/true:
clean_view = arcpy.GetParameterAsText(4)
if str(clean_view) == 'true':
	clean_spatial_view = 'yes'
else:
	clean_spatial_view = 'no'

# specify start and end date to create spatial views for:
start_date_in = arcpy.GetParameterAsText(5)
start_date = str(start_date_in)
end_date_in = arcpy.GetParameterAsText(6)
end_date = str(end_date_in)


# determine which states to create spatial views for.  must use comma separated list.  selecting all_state variable will generate views for all 50 states.
state_string = arcpy.GetParameterAsText(7)
all_state = arcpy.GetParameterAsText(8)

# auto add created layers to TOC.  Default itrue
auto_add = arcpy.GetParameterAsText(9)
if str(auto_add) == 'true':
	add_layers = 'yes'
else:
	add_layers = 'no'

# # define environment settings
# space = 'Database Connections\spatial_view.sde'
# arcpy.env.workspace = space
# mxd = arcpy.mapping.MapDocument(r"CURRENT")
# df = arcpy.mapping.ListDataFrames(mxd,'*')[0]


# Generate Zip Table	
if zip_on == 'yes':
	zip_table = 'tblz_' + table_name
	zip_view = 'svwz_' + table_name
	zip_layer = 'svwz_' + table_name
	
	# Generate table for all states	
	if str(all_state) == 'true':
		all_state_on = 'yes'
		con = msparkdb.dbconn()
		cur = con.cursor()
		PK = 'PK_' + zip_table
		sql_command = """select * into %s from [map_data].dbo.tblmappingzipdata_cp a where a.inhome_date >= ? and a.inhome_date <= ? or event_type = 'solo' """ % zip_table
		sql_command_2 = """ALTER TABLE %s ADD ID INT IDENTITY, selected bit,userSelect varchar(50),store varchar(50),distance float""" % zip_table
		sql_command_6 = """ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY(ID)""" % (zip_table,PK)	
		sql_command_3 = """UPDATE %s SET selected = 0""" % zip_table
		sql_command_4 = """	CREATE VIEW %s
							AS
							SELECT a.*,b.OBJECTID, b.SHAPE
							FROM %s a
							JOIN [map_data].dbo.ZIP_POLYGON b
							ON a.zip = b.ZIP""" % (zip_view, zip_table)
		date_list = list()
		date_list.append(start_date)
		date_list.append(end_date)
		params = date_list
		cur.execute(sql_command,params)	
		cur.execute(sql_command_2)
		cur.execute(sql_command_6)
		cur.execute(sql_command_3)
		cur.execute(sql_command_4)
		con.commit()
		con.close()

	# Generate table for listed states	
	else:
		all_state_on = 'no'
		state_string = state_string.upper()
		state_tuple = tuple(state_string.split(','))
		con = msparkdb.dbconn()
		cur = con.cursor()
		ph = ",".join("?" * len(state_tuple))
		PK = 'PK_' + zip_table
		sql_command = """select * into %s from [map_data].dbo.tblmappingzipdata_cp a where a.[state] in (%s) and (a.inhome_date >= %s and a.inhome_date <= %s or a.event_type = 'solo')""" % (zip_table, ph,"'"+start_date+"'","'"+end_date+"'")
		sql_command_2 = """ALTER TABLE %s ADD ID INT IDENTITY, selected bit,userSelect varchar(50),store varchar(50),distance float""" % zip_table
		sql_command_6 = """ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY(ID)""" % (zip_table,PK)	
		sql_command_3 = """UPDATE %s SET selected = 0""" % zip_table
		sql_command_4 = """CREATE VIEW %s
							AS
							SELECT a.*,b.OBJECTID, b.SHAPE
							FROM %s a
							JOIN [map_data].dbo.ZIP_POLYGON b
							ON a.zip = b.ZIP""" % (zip_view, zip_table)
		cur.execute(sql_command,state_tuple)
		cur.execute(sql_command_2)
		cur.execute(sql_command_6)
		cur.execute(sql_command_3)
		cur.execute(sql_command_4)
		con.commit()
		con.close()

		
# Generate Split Table	
if split_on == 'yes':
	split_table = 'tbls_' + table_name
	split_view = 'svws_' + table_name
	split_layer = 'svws_' + table_name
	
	# Generate table for all states	
	if str(all_state) == 'true':
		all_state_on = 'yes'
		con = msparkdb.dbconn()
		cur = con.cursor()
		PK = 'PK_' + split_table
		sql_command = """select * into %s from [map_data].dbo.tblmappingsplitzipdata_cp a where a.inhome_date >= ? and a.inhome_date <= ? or event_type = 'solo' """ % split_table
		sql_command_2 = """ALTER TABLE %s ADD ID INT IDENTITY, selected bit,userSelect varchar(50),store varchar(50),distance float""" % split_table
		sql_command_6 = """ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY(ID)""" % (split_table,PK)	
		sql_command_3 = """UPDATE %s SET selected = 0""" % split_table
		sql_command_4 = """	CREATE VIEW %s
							AS
							SELECT a.*,b.OBJECTID, b.SHAPE
							FROM %s a
							JOIN [map_data].dbo.TPZ_POLYGON b
							ON a.zip_split = b.tpz""" % (split_view, split_table)
		date_list = list()
		date_list.append(start_date)
		date_list.append(end_date)
		params = date_list
		cur.execute(sql_command,params)	
		cur.execute(sql_command_2)
		cur.execute(sql_command_6)
		cur.execute(sql_command_3)
		cur.execute(sql_command_4)
		con.commit()
		con.close()

	# Generate table for listed states	
	else:
		all_state_on = 'no'
		state_string = state_string.upper()
		state_tuple = tuple(state_string.split(','))
		con = msparkdb.dbconn()
		cur = con.cursor()
		ph = ",".join("?" * len(state_tuple))
		PK = 'PK_' + split_table
		sql_command = """select * into %s from [map_data].dbo.tblmappingsplitzipdata_cp a where a.[state] in (%s) and (a.inhome_date >= %s and a.inhome_date <= %s or a.event_type = 'solo')""" % (split_table, ph,"'"+start_date+"'","'"+end_date+"'")
		sql_command_2 = """ALTER TABLE %s ADD ID INT IDENTITY, selected bit,userSelect varchar(50),store varchar(50),distance float""" % split_table
		sql_command_6 = """ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY(ID)""" % (split_table,PK)	
		sql_command_3 = """UPDATE %s SET selected = 0""" % split_table
		sql_command_4 = """CREATE VIEW %s
							AS
							SELECT a.*,b.OBJECTID, b.SHAPE
							FROM %s a
							JOIN [map_data].dbo.TPZ_POLYGON b
							ON a.zip_split = b.tpz""" % (split_view, split_table)
		cur.execute(sql_command,state_tuple)
		cur.execute(sql_command_2)
		cur.execute(sql_command_6)
		cur.execute(sql_command_3)
		cur.execute(sql_command_4)
		con.commit()
		con.close()


# Generate route Table	
if route_on == 'yes':
	route_table = 'tblr_' + table_name
	route_view = 'svwr_' + table_name
	route_layer = 'svwr_' + table_name
	
	# Generate table for all states	
	if str(all_state) == 'true':
		all_state_on = 'yes'
		con = msparkdb.dbconn()
		cur = con.cursor()
		PK = 'PK_' + route_table
		sql_command = """select * into %s from [map_data].dbo.tblmappingroutedata_cp a where a.inhome_date >= ? and a.inhome_date <= ? or event_type = 'solo' """ % route_table
		sql_command_2 = """ALTER TABLE %s ADD ID INT IDENTITY, selected bit,userSelect varchar(50),store varchar(50),distance float""" % route_table
		sql_command_6 = """ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY(ID)""" % (route_table,PK)	
		sql_command_3 = """UPDATE %s SET selected = 0""" % route_table
		sql_command_4 = """	CREATE VIEW %s
							AS
							SELECT a.*,b.OBJECTID, b.SHAPE
							FROM %s a
							JOIN [map_data].dbo.ROUTE_POLYGON b
							ON a.cr_id = b.cr_id""" % (route_view, route_table)
		date_list = list()
		date_list.append(start_date)
		date_list.append(end_date)
		params = date_list
		cur.execute(sql_command,params)	
		cur.execute(sql_command_2)
		cur.execute(sql_command_6)
		cur.execute(sql_command_3)
		cur.execute(sql_command_4)
		con.commit()
		con.close()

	# Generate table for listed states	
	else:
		all_state_on = 'no'
		state_string = state_string.upper()
		state_tuple = tuple(state_string.split(','))
		con = msparkdb.dbconn()
		cur = con.cursor()
		ph = ",".join("?" * len(state_tuple))
		PK = 'PK_' + route_table
		sql_command = """select * into %s from [map_data].dbo.tblmappingroutedata_cp a where a.[state] in (%s) and (a.inhome_date >= %s and a.inhome_date <= %s or a.event_type = 'solo')""" % (route_table, ph,"'"+start_date+"'","'"+end_date+"'")
		sql_command_2 = """ALTER TABLE %s ADD ID INT IDENTITY, selected bit,userSelect varchar(50),store varchar(50),distance float""" % route_table
		sql_command_6 = """ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY(ID)""" % (route_table,PK)	
		sql_command_3 = """UPDATE %s SET selected = 0""" % route_table
		sql_command_4 = """CREATE VIEW %s
							AS
							SELECT a.*,b.OBJECTID, b.SHAPE
							FROM %s a
							JOIN [map_data].dbo.ROUTE_POLYGON b
							ON a.cr_id = b.cr_id""" % (route_view, route_table)
		cur.execute(sql_command,state_tuple)
		cur.execute(sql_command_2)
		cur.execute(sql_command_6)
		cur.execute(sql_command_3)
		cur.execute(sql_command_4)
		con.commit()
		con.close()

# clean views
if clean_spatial_view == 'yes':
	if zip_on == 'yes':	
		msparkdb.cleanview(zip_table)
	if split_on == 'yes':
		msparkdb.cleanview(split_table)
	if route_on == 'yes':
		msparkdb.cleanview(route_table)

# add view layers to TOC	
db_connection = 'Database Connections\spatial_view.sde'	
arcpy.RefreshCatalog(db_connection)	

if add_layers == 'yes':
	if zip_on == 'yes':
		msparkdb.importview(zip_view)
	if split_on == 'yes':
		msparkdb.importview(split_view)
	if route_on == 'yes':
		msparkdb.importview(route_view)

	
	arcpy.RefreshTOC()
	arcpy.RefreshActiveView()




	