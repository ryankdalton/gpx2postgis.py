# gpx2postgis.py
Python script that uses the OGR API to convert GPX files to PostGIS format.  This script will create a blank PostGIS table with the schema imported from the appropriate GPX sublayer ("waypoints", "routes", "route_points", "tracks", "track_points") is found and does not already exist.  
Once a new table exists, ne script will loop through all selected GPX files in a folder, and loop through all sublayers in each GPX file, and append features to the correct table.
