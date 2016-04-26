package com.conveyal.datatools.editor.controllers;

import com.conveyal.datatools.editor.datastore.GlobalTx;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.conveyal.datatools.editor.models.Account;

public class Security extends Secure.Security {

    static boolean authenticate(String username, String password) {
        GlobalTx tx = VersionedDataStore.getGlobalTx();
        
        try {
            return tx.accounts.containsKey(username) && tx.accounts.get(username).checkPassword(password);
        }
        finally {
            tx.rollback();
        }
        
    }
    
    static boolean check(String profile) {
        return session.contains("manageableFeeds") && session.get("manageableFeeds").length() > 0;
        /*GlobalTx tx = VersionedDataStore.getGlobalTx();
        
        try {
            if("admin".equals(profile))
                return tx.accounts.containsKey(connected()) && tx.accounts.get(connected()).isAdmin();
            else
                return false;
        }
        finally {
            tx.rollback();
        }*/
    }
 
    static Account getAccount()
    {
        GlobalTx tx = VersionedDataStore.getGlobalTx();
        try {
            return tx.accounts.get(connected());
        }
        finally {
            tx.rollback();
        }
    }
    
}