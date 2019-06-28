package com.conveyal.datatools.manager.auth;

/**
 * The set of request actions that a user can take on application entities. These are checked
 * against the requesting user's permissions to ensure that they have permission to make the request.
 */
public enum Actions {
    CREATE, EDIT, MANAGE, VIEW
}
