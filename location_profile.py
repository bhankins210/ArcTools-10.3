import arcpy
import pypyodbc


# Create empty locations table
connection_string = 'DRIVER={SQL Server};SERVER=MS-SQL2;DATABASE=mapping;UID=sdeadmin;PWD=theo'
con = pypyodbc.connect(connection_string)
cur = con.cursor()
table_name = 'R64609911_LOC_TEST'

query = "CREATE TABLE %s (ID int IDENTITY(1,1) PRIMARY KEY, store varchar(50) NOT NULL, latitude float, longitude float);" % table_name
cur.execute(query)
#cur.execute("""CREATE TABLE '?' (ID int IDENTITY(1,1) PRIMARY KEY, store varchar(50) NOT NULL, latitude float, longitude float);""",table_name)				
con.commit()
con.close()




# Insert Store data into table
connection_string = 'DRIVER={SQL Server};SERVER=MS-SQL2;DATABASE=mapping;UID=sdeadmin;PWD=theo'
con = pypyodbc.connect(connection_string)
cur = con.cursor()	
featureclass = 'R64609911_LOC'
table_name = featureclass + '_TEST'
rows = arcpy.SearchCursor(featureclass)
row = rows.next()
store_lst = list()
while row:
	store = row.store
	lat = row.latitude
	lon = row.longitude
	app = store, str(lat),str(lon)
	store_lst.append(app)
	row = rows.next()
params = store_lst
query = "INSERT INTO %s (store,latitude,longitude) VALUES (?,?,?);" % table_name
cur.executemany(query,params)
con.commit()
con.close()





# USE THIS!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# profile without duplication 
connection_string2 = 'DRIVER={SQL Server};SERVER=MS-SQL2;DATABASE=mapping;UID=sdeadmin;PWD=theo'
con2 = pypyodbc.connect(connection_string2)
cur2 = con2.cursor()	
featureclass = 'R64609911_LOC'
rows = arcpy.SearchCursor(featureclass)
row = rows.next()
radius = 100
dupes = 'no'
tbl = 'tblz_r64609911_DUP_B'
while row:
	store = row.store
	lat = row.latitude
	lon = row.longitude
	sql_command = 'EXEC [sdeadmin].[gis_SearchRadius] ' + "'" + tbl + "'" + ', ' + "'" + str(lat) + "'" + ', ' + "'" +  str(lon) + "'" + ', ' + "'" + str(radius) + "'" +', ' + "'" + store + "'" + ', ' + "'" + dupes + "'"
	#print sql_command
	cur2.execute(sql_command)
	con2.commit()
	row = rows.next()
con2.close()
	
# profile with duplication 
connection_string2 = 'DRIVER={SQL Server};SERVER=MS-SQL2;DATABASE=mapping;UID=sdeadmin;PWD=theo'
con2 = pypyodbc.connect(connection_string2)
cur2 = con2.cursor()	
featureclass = 'R64609911_LOC'
rows = arcpy.SearchCursor(featureclass)
row = rows.next()
radius = 20
dupes = 'no'
tbl = 'tblz_r64609911_DUP_B'
while row:
	store = row.store
	lat = row.latitude
	lon = row.longitude
	sql_command = 'EXEC [sdeadmin].[gis_RadiusDupes] ' + "'" + tbl + "'" + ', ' + "'" + str(lat) + "'" + ', ' + "'" +  str(lon) + "'" + ', ' + "'" + str(radius) + "'" +', ' + "'" + store + "'" + ', ' + "'" + dupes + "'"
	#print sql_command
	cur2.execute(sql_command)
	con2.commit()
	row = rows.next()
con2.close()
		

	
	
	



connection_string = 'DRIVER={SQL Server};SERVER=MS-SQL2;DATABASE=mapping;UID=sdeadmin;PWD=theo'
con = pypyodbc.connect(connection_string)
cur = con.cursor()	
cur.execute("""
			DECLARE @distance_unit float
			DECLARE @radius float
			SET @radius = 10.0
			SET @distance_unit = 69.0


			--create temp tables for testing
			select * into #view from tblz_r64609911
			--alter table #view drop column distance,store



			--create table with great earth distance joining polygon table with view table
			SELECT store, a.zip, --a.latitude, a.longitude,
				  111.045* DEGREES(ACOS(COS(RADIANS(latpoint))
							 * COS(RADIANS(latitude))
							 * COS(RADIANS(longpoint) - RADIANS(longitude))
							 + SIN(RADIANS(latpoint))
							 * SIN(RADIANS(latitude))))*0.62137 AS distance_in_mi
			 into #distance
			 FROM zip_polygon a
			 JOIN (
				 SELECT  store, latitude as latpoint,  longitude AS longpoint, @radius as radius, @distance_unit as distance_unit 
				 from R64609911_LOC_TEST
					) AS p ON 1=1
			WHERE (a.latitude 
				 BETWEEN p.latpoint  - (p.radius / p.distance_unit)
					 AND p.latpoint  + (p.radius / p.distance_unit)
				AND a.longitude
				 BETWEEN p.longpoint - (p.radius / (p.distance_unit * COS(RADIANS(p.latpoint))))
					 AND p.longpoint + (p.radius / (p.distance_unit * COS(RADIANS(p.latpoint))))) and	
			a.zip in (select zip from #view)  
			ORDER BY distance_in_mi

			------deletes duplication and leaves zip assigned to closest store
			--DELETE a FROM #distance a
			--JOIN (SELECT zip, min(distance_in_mi) AS max_dist
			--FROM #distance
			--group by zip) b on a.zip = b.zip and a.distance_in_mi <> b.max_dist


			--join distance table with view table
			select a.*, b.store as store, b.distance_in_mi as distance
			into #loc_test
			from #view a
			join #distance  b on a.zip = b.zip
			order by a.zip, b.distance_in_mi


			SET IDENTITY_INSERT tblz_r64609911 OFF
			insert into tblz_r64609911
			select zip,city,company_code,company_name,dma,dma_name,event_id,event_type,facility_id,fips,
					freq_code,hh_count,inhome_date,market_code,msa,msa_name,ranking,state,wrap_zone,
					market,first_run_date,notes,selected,userSelect,store,distance
			from #loc_test

			DELETE FROM tblz_r64609911
			WHERE store IS NULL and zip in (select zip from #loc_test)


			drop table #view
			drop table #loc_test
			drop table #distance
""")

con.close()
	

	
	
	
