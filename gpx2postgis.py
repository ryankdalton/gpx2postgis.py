#-------------------------------------------------------------------------------
# Name:        gpx2postgis
# Purpose:     Batch load GPX files into a PostGIS database.
#              Also allows the user to specify a DB and schema to load data
#              into.  The script will automatically create an empty table with
#              the schema of the specified GPX feature type (if one does not
#              already exist).  Features are append the appropriate table of
#              the same name as the GPX layer.
#
# Author:      rdalton
#
# Created:     13/06/2016
# Copyright:   None
# Licence:     None
#-------------------------------------------------------------------------------
import os, subprocess, glob
from osgeo import ogr


#Define the path to files and file format to import
inFolder = r'D:\path\to\files'
inFormat = "gpx"

#Define the GPX feature types to import
gpxImportLayers = ["waypoints", "routes", "route_points", "tracks", "track_points"]

#Define PostGIS connection details
dbFormat = "PostgreSQL"
dbHost = "localhost"
dbName = "postgis"
dbSchema = "gps"
dbUser = "XXXX"
dbPWD = "XXXX"


def createPostgisConnection(dbFormat, dbHost, dbName, dbSchema, dbUser, dbPWD):
    """Establish a connection to a PostGIS database"""

    pg = ogr.GetDriverByName(dbFormat)
    if pg is None:
        raise RuntimeError('{0} driver not available'.format(dbFormat))
    conn = pg.Open("PG:dbname='{0}' user='{1}' password='{2}'".format(dbName, dbUser, dbPWD), True)
    if conn is None:
        raise RuntimeError('Cannot open dataset connection')
    return conn




def importGPX(gpxFile, gpxLayerFilter, pgConn):
    """Import features from a GPX file into PostGIS"""

    def ogrCreateLayer(sourceLayer, pgConn, destinationLayer):
        """Create an empty table with the schema derived from the GPX feature type"""
        print "  Creating {0}".format(destinationLayer)
        newLayer = pgConn.CreateLayer(destinationLayer)

        lyrDefn = sourceLayer.GetLayerDefn()
        for i in range( lyrDefn.GetFieldCount() ):
            ##print "Creating field: {0}".format(lyrDefn.GetFieldDefn( i ).GetName())

            fieldName = lyrDefn.GetFieldDefn( i ).GetName()
            fieldType = lyrDefn.GetFieldDefn( i ).GetType()
            newField = ogr.FieldDefn(fieldName, fieldType)
            newLayer.CreateField(newField)

    def ogrAppendFeatures(gpxFile, sourceLayer, destinationLayer):
        """Append GPX features to existing table based on feature type"""

        ##print "Starting transaction for: {0}".format(destinationLayer.GetName())
        print "  Importing {0}:  {1} features".format(sourceLayer.GetName(), sourceLayer.GetFeatureCount())
        fName = os.path.basename(gpxFile)
        destinationLayer.StartTransaction()
        for x in xrange(sourceLayer.GetFeatureCount()):
            sourceFeature = sourceLayer.GetNextFeature()
            ##print "inserting record"
            sourceFeature.SetFID(-1)
            sourceFeature.SetField("src", fName)
            destinationLayer.CreateFeature(sourceFeature)

        #Commit the new features to the database
        ##print "    Committing transaction for: {0}".format(destinationLayer.GetName())
        destinationLayer.CommitTransaction()




    # Establish a connection to a GPX file
    print "Reading {0}".format(gpxFile)
    try:
        datasource = ogr.Open(gpxFile)
        if datasource == None:
            print "  WARNING: Could not read {0}.  Skipping...".format(gpxFile)
            return None

        for i in range(datasource.GetLayerCount()):
             ##print datasource.GetLayer(i).GetName()," :", datasource.GetLayer(i).GetFeatureCount()

            if datasource.GetLayer(i).GetName() not in gpxLayerFilter:
                print "  Skipping {0}:  User filtered".format(datasource.GetLayer(i).GetName())
            elif datasource.GetLayer(i).GetFeatureCount() == 0:
                print "  Skipping {0}:  0 features".format(datasource.GetLayer(i).GetName())
            else:
                inLayer = datasource.GetLayer(i)
                outLayer = "{0}.{1}".format(dbSchema, inLayer.GetName())

                if pgConn.GetLayerByName(outLayer) == None:
                    #If layer does not already exist, copy source schema + features to destination
                    ogrCreateLayer(inLayer, pgConn, outLayer)

                #If layer does exist, copy source **features** to the destination
                ogrAppendFeatures(gpxFile, inLayer,pgConn.GetLayerByName(outLayer))

        del datasource


    except Exception, e:
        print e.args






def main():
    """Import a list of GPX files in a folder, and import them into PostGIS"""

    #Create a list of all files that have the GPX file format
    fileList = glob.glob(os.path.join(inFolder,"*.{0}".format(inFormat)))

    #Create a connection to PostGIS database
    pgConn = createPostgisConnection(dbFormat, dbHost, dbName, dbSchema, dbUser, dbPWD)

    #Process each *listed* layer type from a GPS file
    for f in fileList:
        importGPX(f, gpxImportLayers, pgConn)


if __name__ == '__main__':
    main()
