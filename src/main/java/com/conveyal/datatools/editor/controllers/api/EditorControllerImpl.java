package com.conveyal.datatools.editor.controllers.api;

import com.conveyal.gtfs.loader.Table;
import com.conveyal.gtfs.model.Entity;

import javax.sql.DataSource;

public class EditorControllerImpl extends EditorController {
    public EditorControllerImpl(String apiPrefix, Table table, DataSource dataSource){
        super(apiPrefix, table, dataSource);
    }
}
