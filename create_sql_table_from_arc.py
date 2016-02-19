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


