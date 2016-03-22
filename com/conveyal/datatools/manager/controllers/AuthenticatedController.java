package com.conveyal.datatools.manager.controllers;

import static spark.Spark.before;

/**
 * Created by demory on 3/14/16.
 */
public class AuthenticatedController {
    {
        System.out.println("auth cont");
        before((request, response) -> {
            /*boolean authenticated;
            // ... check if authenticated
            if (!authenticated) {
                halt(401, "You are not welcome here");
            }*/
            System.out.println("auth check");
        });
    }

}
