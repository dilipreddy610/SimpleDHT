package edu.buffalo.cse.cse486586.simpledht;

/**
 * Created by dilip on 3/4/2018.
 */

import android.content.Context;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * Created by dilip on 2/15/2018.
 *
 */
///ref:https://developer.android.com/reference/android/database/sqlite/SQLiteOpenHelper.html
public class GroupMessengerDB extends SQLiteOpenHelper {

    static final String DataBaseName="PA2.db";
    static final int Version=2;
    static final String tableName="messages_saved";

    public GroupMessengerDB(Context context){
        super(context,DataBaseName,null,Version);
    }
    @Override
    public void onCreate(SQLiteDatabase sqLiteDatabase) throws SQLException
    {
        String query="CREATE TABLE "+tableName+"(key VARCHAR PRIMARY KEY,value VARCHAR);";
        sqLiteDatabase.execSQL(query);
    }

    @Override
    public void onUpgrade(SQLiteDatabase sqLiteDatabase,int x,int y){

    }
}