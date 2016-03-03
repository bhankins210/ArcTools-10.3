"""
database connection file

"""

import pypyodbc

def dbconn():
    try:
        connection = pypyodbc.connect('DRIVER={SQL Server};SERVER=mapping-sqldev\esri;DATABASE=spatial_view;UID=id;PWD=pass;Trusted_Connection=Yes')
        return connection
    except:
        print 'Cannot connect to database'
def dbcursor():
	try:
		con = dbconn()
		cursor = con.cursor()
		return cursor
	except:
		print 'Cannot create cursor'