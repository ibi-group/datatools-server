package com.conveyal.datatools.editor.jobs;


public class ProcessGisExport implements Runnable {
    @Override
    public void run() {

    }
/*
    private Long _gisExportId;


    public ProcessGisExport(Long gisExportId)
    {
        this._gisExportId = gisExportId;
    }

    public void doJob() {

        String exportName = "gis_" + this._gisExportId;

        File outputZipFile = new File(Play.configuration.getProperty("application.publicDataDirectory"), exportName + ".zip");

        File outputDirectory = new File(Play.configuration.getProperty("application.publicDataDirectory"), exportName);

        LOG.info("outfile path:" + outputDirectory.getAbsolutePath());

        File outputShapefile = new File(outputDirectory, exportName + ".shp");
       
        try
        {
            GisExport gisExport = null;

            while(gisExport == null)
            {
                gisExport = GisExport.findById(this._gisExportId);
                Thread.sleep(1000);

                LOG.info("Waiting for gisExport object...");
            }


            if(!outputDirectory.exists())
            {
                outputDirectory.mkdir();
            }

            ShapefileDataStoreFactory com.conveyal.datatools.editor.datastoreFactory = new ShapefileDataStoreFactory();

            Map<String, Serializable> params = new HashMap<String, Serializable>();
            params.put("url", outputShapefile.toURI().toURL());
            params.put("create spatial index", Boolean.TRUE);

            ShapefileDataStore com.conveyal.datatools.editor.datastore = (ShapefileDataStore)dataStoreFactory.createNewDataStore(params);
            com.conveyal.datatools.editor.datastore.forceSchemaCRS(DefaultGeographicCRS.WGS84);

            SimpleFeatureType STOP_TYPE = DataUtilities.createType(
                    "Stop",                 
                    "location:Point:srid=4326," + 
                    "name:String," +
                    "code:String," +
                    "desc:String," +
                    "id:String," +
                    "agency:String"     
            );

            SimpleFeatureType ROUTE_TYPE = DataUtilities.createType(
                    "Route",                   // <- the name for our feature type
                    "route:LineString:srid=4326," +
                    "patternName:String," +
                    "shortName:String," +
                    "longName:String," +
                    "desc:String," +
                    "type:String," +
                    "url:String," +
                    "routeColor:String," +
                    "routeTextColor:String," +
                    "agency:String"     
            );

            SimpleFeatureCollection collection;

            SimpleFeatureType collectionType;

            SimpleFeatureBuilder featureBuilder = null;
            
            List<SimpleFeature> features = new ArrayList<SimpleFeature>();
            
            if(gisExport.type.equals(GisUploadType.STOPS))
            {
                collectionType = STOP_TYPE;
                com.conveyal.datatools.editor.datastore.createSchema(STOP_TYPE);
                featureBuilder = new SimpleFeatureBuilder(STOP_TYPE);

                List<Stop> stops = Stop.find("agency in (:ids)").bind("ids", gisExport.agencies).fetch();

                for(Stop s : stops)
                {
                    featureBuilder.add(s.locationPoint());
                    featureBuilder.add(s.stopName);
                    featureBuilder.add(s.stopCode);
                    featureBuilder.add(s.stopDesc);
                    featureBuilder.add(s.gtfsStopId);
                    featureBuilder.add(s.agency.name);
                    SimpleFeature feature = featureBuilder.buildFeature(null);
                    features.add(feature);
                }
            }
            else if(gisExport.type.equals(GisUploadType.ROUTES))
            {
                collectionType = ROUTE_TYPE;
                com.conveyal.datatools.editor.datastore.createSchema(ROUTE_TYPE);
                featureBuilder = new SimpleFeatureBuilder(ROUTE_TYPE);

                List<Route> routes = Route.find("agency in (:ids)").bind("ids", gisExport.agencies).fetch();

                // check for duplicates

                // HashMap<String, Boolean> existingRoutes = new HashMap<String,Boolean>();
                
                for(Route r : routes)
                {
//                    String routeId = r.routeLongName + "_" + r.routeDesc + "_ " + r.phone.id;
//
//                    if(existingRoutes.containsKey(routeId))
//                        continue;
//                    else
//                        existingRoutes.put(routeId, true);


                    List<TripPattern> patterns = TripPattern.find("route = ?", r).fetch();
                    for(TripPattern tp : patterns)
                    {
                        if(tp.shape == null)
                            continue;

                        featureBuilder.add(tp.shape.shape);
                        featureBuilder.add(tp.name);
                        featureBuilder.add(r.routeShortName);
                        featureBuilder.add(r.routeLongName);
                        featureBuilder.add(r.routeDesc);

                        if(r.routeType != null)
                            featureBuilder.add(r.routeType.toString());
                        else
                            featureBuilder.add("");

                        featureBuilder.add(r.routeUrl);
                        featureBuilder.add(r.routeColor);
                        featureBuilder.add(r.routeTextColor);
                        featureBuilder.add(r.agency.name);
                        SimpleFeature feature = featureBuilder.buildFeature(null);
                        features.add(feature);
                    }
                }
            }
            else
                throw new Exception("Unknown export type.");

            collection = new ListFeatureCollection(collectionType, features);
            
            Transaction transaction = new DefaultTransaction("create");

            String typeName = com.conveyal.datatools.editor.datastore.getTypeNames()[0];
            SimpleFeatureSource featureSource = com.conveyal.datatools.editor.datastore.getFeatureSource(typeName);

            if (featureSource instanceof SimpleFeatureStore) 
            {
                SimpleFeatureStore featureStore = (SimpleFeatureStore) featureSource;

                featureStore.setTransaction(transaction);
               
                featureStore.addFeatures(collection);
                transaction.commit();

                transaction.close();
            } 
            else 
            {
                throw new Exception(typeName + " does not support read/write access");
            }
            
            DirectoryZip.zip(outputDirectory, outputZipFile);
            FileUtils.deleteDirectory(outputDirectory);

            gisExport.status = GisExportStatus.PROCESSED;

            gisExport.save();
            
        }
        catch(Exception e)
        {
            LOG.error("Unable to process GIS export: ", e.toString());
            e.printStackTrace();
        } 
    }*/
}


