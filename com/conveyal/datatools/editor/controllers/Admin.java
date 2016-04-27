package com.conveyal.datatools.editor.controllers;

import com.conveyal.datatools.editor.datastore.GlobalTx;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.conveyal.datatools.editor.models.Account;
import com.conveyal.datatools.editor.models.transit.Agency;

import spark.Request;
import spark.Response;

import static spark.Spark.*;

import java.util.Collection;


public class Admin {


//    static void setConnectedUser() {
//        if(Security.isConnected() && Security.check("admin")) {
//            renderArgs.put("user", Security.connected());
//        }
//        else
//            Application.index();
//    }


    public static void accounts() {
        GlobalTx tx = VersionedDataStore.getGlobalTx();

        try {
            Collection<Account> accounts = tx.accounts.values();
            Collection<Agency> agencies = tx.agencies.values();

//            render(accounts, agencies);
        } finally {
            tx.rollback();
        }
    }

    public static void createAccount(String username, String password, String email, Boolean admin, String agencyId)
    {
        GlobalTx tx = VersionedDataStore.getGlobalTx();

        if(!username.isEmpty() && !password.isEmpty() && !email.isEmpty() && !tx.accounts.containsKey(username)) {
            Account acct = new Account(username, password, email, admin, agencyId);
            tx.accounts.put(acct.id, acct);
            tx.commit();
        }
        else {
            tx.rollback();
        }

        Admin.accounts();
    }

    public static void updateAccount(String username, String email, Boolean active, Boolean admin, Boolean taxi, Boolean citom, String agencyId)
    {
        GlobalTx tx = VersionedDataStore.getGlobalTx();

        if (!tx.accounts.containsKey(username)) {
            halt(400);
            return;
        }

        Account acct = tx.accounts.get(username).clone();
        acct.email = email;
        acct.active = active;
        acct.admin = admin;
        acct.agencyId = agencyId;

        tx.accounts.put(acct.id, acct);

        tx.commit();

        Admin.accounts();
    }

    public static void getAccount(String username) {

        GlobalTx tx = VersionedDataStore.getGlobalTx();
        Account account = tx.accounts.get(username);
        tx.rollback();

        if (account == null)
            halt(404);
//        else
//            renderJSON(account);
    }


    public static Boolean checkUsername(String username) {
        GlobalTx tx = VersionedDataStore.getGlobalTx();
        boolean exists = tx.accounts.containsKey(username);
        tx.rollback();

        if (exists) {
            halt(400);
            return false;
        }
        else
            return true; // ok();

    }

    public static Boolean resetPassword(String username, String newPassword)
    {
        GlobalTx tx = VersionedDataStore.getGlobalTx();
        if (!tx.accounts.containsKey(username)) {
            tx.rollback();
            halt(404);
            return false;
        }

        Account acct = tx.accounts.get(username).clone();
        acct.updatePassword(newPassword);
        tx.accounts.put(acct.id, acct);

        tx.commit();

        return true; // ok();

    }

}