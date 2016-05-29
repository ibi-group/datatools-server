package com.conveyal.datatools.editor.models;

import java.io.Serializable;

public class OAuthToken extends Model implements Serializable {
    public static final long serialVersionUID = 1;

    public final String token;
    public long creationDate;
    public String agencyId;
    
    public OAuthToken (String token, String agencyId) {
        this.id = Account.hash(token);
        this.token = Account.hash(token);
        this.agencyId = agencyId;
        this.creationDate = System.currentTimeMillis();
    }
}
