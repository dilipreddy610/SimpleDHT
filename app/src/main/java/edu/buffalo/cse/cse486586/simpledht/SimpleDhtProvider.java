package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.Map;
import java.util.TreeMap;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;

public class SimpleDhtProvider extends ContentProvider {

    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    static String nodeHash;
    static String portStr;
    static String myPort;
    static String prdcrNode;
    static String sucrNode="First";
    static String msgToSend;
    static String prdcrHash;
    static String sucrHash;
    static String keyHash;
    static String selectionHash;
    static int head=0;
    static String origin="x";
    // Ref: https://docs.oracle.com/javase/7/docs/api/java/util/TreeMap.html
    TreeMap<String,String> nodes_created=new TreeMap<String, String>();
    // Reference: https://developer.android.com/training/data-storage/sqlite.html
    SQLiteDatabase sqlDb;
    GroupMessengerDB db;
    static String KEY="key";
    static String VALUE="value";
    static String[] portArray={"11108","11112","11116","11120","11124"};


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        int i=0;
        String selhash="";
        try {
            selhash = genHash(selection);
        }catch (NoSuchAlgorithmException e){
            Log.e(TAG, "Hash function not defined: ", e);
        }
        db= new GroupMessengerDB(this.getContext());
        sqlDb=db.getWritableDatabase();
        if(selection.equals("*")){
            i= sqlDb.delete("messages_saved",null,null);
        }
        else if(sucrNode.equalsIgnoreCase("First")){
            i = sqlDb.delete("messages_saved", KEY + "=" +"'"+selection+"'", null);
        }
        else if(head==1)
        {

            if (nodeHash.compareTo(selhash) > 0) {
                i = sqlDb.delete("messages_saved", KEY + "=" +"'"+selection+"'", null);
            }
            else if (prdcrHash.compareTo(selhash) < 0) {
                i = sqlDb.delete("messages_saved", KEY + "=" +"'"+selection+"'", null);
            }
            else {
                String mes = "delete" + ":" + selection;
                try {
                    Socket delete_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(sucrNode));
                    PrintWriter out2 = new PrintWriter(delete_socket.getOutputStream(), true);
                    out2.println(mes);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        }
        else if(nodeHash.compareTo(selhash)>=0&&prdcrHash.compareTo(selhash)<0)
        {
                i=sqlDb.delete("messages_saved",KEY+"="+"'"+selection+"'",null);
        }
        else{
                String mes="delete"+":"+selection;
                try
                {
                    Socket insert_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(sucrNode));
                    PrintWriter out2 = new PrintWriter(insert_socket.getOutputStream(), true);
                    out2.println(mes);
                }catch(Exception e){
                    e.printStackTrace();
                }
            }

            return i;
            }






    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        try {
            keyHash = genHash((values.get("key")).toString());
        }catch(NoSuchAlgorithmException e){
            Log.e(TAG, "Hash function not defined: ", e);
        }
        return uri;
    }

    @Override
    public boolean onCreate() {

        try {
            TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
            portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        }
        catch(NullPointerException e){
            Log.e(TAG, "Unable to get port number: ", e );
        }

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket: ", e);
        }
        try {
            nodeHash = genHash(portStr);
        }
        catch(NoSuchAlgorithmException e){
            Log.e(TAG, "Hash function not defined: ", e);
        }
        nodes_created.put(nodeHash,myPort);
        if(!portStr.equalsIgnoreCase("5554")){
            Log.v("Creating Client Task:",portStr);
            String msg="Join_Request"+":"+nodeHash+":"+myPort;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
        }

        return false;
    }
    Uri uri=makeURI("content","edu.buffalo.cse.cse486586.simpledht.provider");
    private Uri makeURI(String content, String path){

        Uri.Builder uriBuilder=new Uri.Builder();
        uriBuilder.scheme(content);
        uriBuilder.authority(path);
        return uriBuilder.build();

    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        GroupMessengerDB db= new GroupMessengerDB(this.getContext());
        SQLiteDatabase sqlDb=db.getReadableDatabase();
        Cursor values1=null;
        // Reference:: https://developer.android.com/reference/android/database/MatrixCursor.html
        MatrixCursor matCursor = new MatrixCursor(new String[]{KEY, VALUE});
        try {
            selectionHash = genHash(selection);
        }catch(NoSuchAlgorithmException e){
            Log.e(TAG, "Hash function not defined: ", e);
        }
        if ( selection.equals("@") ){
            String[] selArgs={selection};
            // Ref:https://developer.android.com/training/data-storage/sqlite.html
            values1 =sqlDb.query(true,"messages_saved",projection,null,null,null,null,null,null);

        }
        else if(selection.equals("*")){
          if(sucrNode.equalsIgnoreCase("First")){
              values1 =sqlDb.query(true,"messages_saved",projection,null,null,null,null,null,null);
          }
          else{
              Cursor c=sqlDb.query(true,"messages_saved",projection,null,null,null,null,null,null);
              if(c.getCount()!=0 && c!=null){
                  c.moveToFirst();
                  do {
                      String[] cue=new String[2];
                      String key = c.getString(c.getColumnIndex(KEY));
                      String valu = c.getString(c.getColumnIndex(VALUE));
                      cue[0]=key;
                      cue[1]=valu;
                      matCursor.addRow(cue);
                  } while (c.moveToNext());
              }
              String s=sucrNode;
              while(!s.equalsIgnoreCase(myPort)){
                  String message="query"+":"+"*";
                  try {
                      Socket query_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                              Integer.parseInt(s));
                      PrintWriter out2 = new PrintWriter(query_socket.getOutputStream(), true);
                      out2.println(message);
                      BufferedReader in = new BufferedReader(new InputStreamReader(query_socket.getInputStream()));
                      String st = in.readLine();
                      String[] messages = st.split(";");
                      s=messages[0];
                      for (int i=1;i<messages.length;i++) {
                          if (!messages[i].equals("")) {
                              String[] cursorEntry = messages[i].split(":");
                              matCursor.addRow(cursorEntry);
                          }
                      }
                  }catch (Exception e) {
                      e.printStackTrace();
                  }
              }
              values1=matCursor;

          }
        }
        else if(sucrNode.equalsIgnoreCase("First")) {
            String[] selArgs={selection};
            String select= "key" + "=?";
            values1 =sqlDb.query(true,"messages_saved",projection,select,selArgs,null,null,null,null,null);

        }

        else if(head==1){
            if(nodeHash.compareTo(selectionHash)>0){
                String[] selArgs={selection};
                String select= "key" + "=?";
                values1 =sqlDb.query(true,"messages_saved",projection,select,selArgs,null,null,null,null);

            }
            else if(prdcrHash.compareTo(selectionHash)<0){
                String select= "key" + "=?";
                String[] selArgs={selection};
                values1 =sqlDb.query(true,"messages_saved",projection,select,selArgs,null,null,null,null);
            }
            else {
                try {
                    String message = "Query" + ":" + selectionHash + ":" + selection;

                    Socket query_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(sucrNode));
                    PrintWriter out2 = new PrintWriter(query_socket.getOutputStream(), true);
                    out2.println(message);
                    BufferedReader in = new BufferedReader(new InputStreamReader(query_socket.getInputStream()));
                    String s=in.readLine();
                    String[] messages=s.split(";");
                    for (String str : messages) {
                        if (!str.equals("")) {
                            String[] cursorEntry = str.split(":");
                            matCursor.addRow(cursorEntry);
                        }
                    }
                    values1=matCursor;

                } catch (Exception e) {
                    Log.e(TAG, "Unable to forward: ", e);
                }
            }

        }
        else if(nodeHash.compareTo(selectionHash)>=0&&prdcrHash.compareTo(selectionHash)<0){
            String select= "key" + "=?";
            String[] sele={selection};
            values1 =sqlDb.query(true,"messages_saved",projection,select,sele,null,null,null,null);
        }
        else
        {
            try
            {
                String message = "Query" + ":" + selectionHash + ":" + selection;

                Socket insert_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(sucrNode));
                PrintWriter out2 = new PrintWriter(insert_socket.getOutputStream(), true);
                out2.println(message);
                BufferedReader in = new BufferedReader(new InputStreamReader(insert_socket.getInputStream()));
                String s=in.readLine();
                String[] messages=s.split(";");
                for (String str : messages) {
                    if (!str.equals("")) {
                        String[] cursorEntry = str.split(":");
                        matCursor.addRow(cursorEntry);
                    }
                }
                values1=matCursor;

            } catch (Exception e)
            {
                Log.e(TAG, "Unable to forward: ", e);
            }
        }
        return values1;
    }
    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException
    {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void>
    {
        @Override
        protected Void doInBackground(ServerSocket... sockets)
        {
            ServerSocket serverSocket = sockets[0];
            try {
                String strin="";
                while (true) {

                    Socket socket = serverSocket.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    strin=in.readLine();
                    String[] str=strin.split(":");
                    if(str[0].equals("Join_Request"))
                    {
                        nodes_created.put(str[1],str[2]);
                        if(nodes_created.size()==2)
                        {

                            for(int i=0;i<nodes_created.size();i++)
                            {
                                //REf:https://docs.oracle.com/javase/7/docs/api/java/util/Map.html
                                if(str[2].equals(nodes_created.values().toArray()[i]))
                                {

                                    if(i==1){
                                        Map.Entry<String,String> firstentry=nodes_created.firstEntry();
                                        msgToSend="Response to Join"+":"+firstentry.getValue()+":"+firstentry.getValue();
                                        prdcrNode=nodes_created.lastEntry().getValue();
                                        sucrNode=nodes_created.lastEntry().getValue();
                                        prdcrHash=nodes_created.lastEntry().getKey();
                                        sucrHash=nodes_created.lastEntry().getKey();
                                    }
                                    else{
                                        Map.Entry<String,String> lastentry=nodes_created.lastEntry();
                                        msgToSend="Response to Join"+":"+lastentry.getValue()+":"+lastentry.getValue();
                                        prdcrNode=nodes_created.firstEntry().getValue();
                                        sucrNode=nodes_created.firstEntry().getValue();
                                        prdcrHash=nodes_created.firstEntry().getKey();
                                        sucrHash=nodes_created.firstEntry().getKey();
                                    }
                                }
                            }
                        }
                        else{

                            for(int i=0;i<nodes_created.size();i++){
                                if(str[2].equals(nodes_created.values().toArray()[i])){
                                    if(i==0){
                                         msgToSend="Response to Join"+":"+nodes_created.values().toArray()[nodes_created.size()-1]+":"+nodes_created.values().toArray()[1];
                                    }
                                    else if(i==nodes_created.size()-1){
                                        msgToSend="Response to Join"+":"+nodes_created.values().toArray()[i-1]+":"+nodes_created.values().toArray()[0];
                                    }
                                    else{
                                        msgToSend="Response to Join"+":"+nodes_created.values().toArray()[i-1]+":"+nodes_created.values().toArray()[i+1];
                                    }
                                }
                            }

                        }
                        PrintWriter serv_out = new PrintWriter(socket.getOutputStream(), true);
                        serv_out.println(msgToSend);
                        serv_out.flush();

                    }
                    else if(str[0].equalsIgnoreCase("Join_Update"))
                    {
                        if(str[1].equals("sucsr"))
                        {
                            prdcrNode=str[2];

                                try {
                                    prdcrHash = genHash(String.valueOf(Integer.parseInt(str[2])/2));
                                    if(nodeHash.compareTo(prdcrHash)<0){
                                        head=1;
                                    }else{
                                        head=0;
                                    }

                                    } catch (NoSuchAlgorithmException e)
                                    {
                                    e.printStackTrace();
                                }
                            Cursor c=query(uri,null,"@",null,null);
                            if(c.getCount()!=0 && c!=null){
                                //REF: https://developer.android.com/reference/android/database/Cursor.html
                                c.moveToFirst();
                                do {
                                    String key = c.getString(c.getColumnIndex(KEY));
                                    String valu = c.getString(c.getColumnIndex(VALUE));
                                    try{
                                        keyHash=genHash(key);
                                    }catch(NoSuchAlgorithmException e){
                                        e.printStackTrace();
                                    }
                                    if(prdcrHash.compareTo(keyHash)>0){
                                        delete(uri,key,null);
                                        ContentValues val=new ContentValues();
                                        val.put(key,valu);
                                        insert(uri,val);
                                    }
                                } while (c.moveToNext());
                            }

                        }
                        else if(str[1].equals("prdcr"))
                        {
                            sucrNode=str[2];
                            try {
                                sucrHash = genHash(String.valueOf(Integer.parseInt(str[2])/2));
                            } catch (NoSuchAlgorithmException e)
                            {
                                e.printStackTrace();
                            }

                        }
                    }
                    else if(str[0].equalsIgnoreCase("Insertion")){
                            ContentValues val=new ContentValues();
                            val.put(VALUE,str[3]);
                            val.put(KEY,str[2]);
                            insert(uri,val);
                    }
                    else if(str[0].equalsIgnoreCase("Query"))
                    {
                        if(str.length==3){
                        Cursor c=query(uri,null,str[2],null,null);
                        String cuemess="";
                            if(c.getCount()!=0 && c!=null){
                            c.moveToFirst();
                            do {
                                String key = c.getString(c.getColumnIndex(KEY));
                                String valu = c.getString(c.getColumnIndex(VALUE));
                                cuemess = cuemess + key + ":" + valu + ";";
                            } while (c.moveToNext());
                        }
                        PrintWriter serv_out = new PrintWriter(socket.getOutputStream(), true);
                        serv_out.println(cuemess);
                        serv_out.flush();
                    }
                    else{
                            Cursor c=query(uri,null,"@",null,null);
                            String cuemess=sucrNode+";";
                            if(c.getCount()!=0 && c!=null){
                                c.moveToFirst();
                                do {
                                    String key = c.getString(c.getColumnIndex(KEY));
                                    String valu = c.getString(c.getColumnIndex(VALUE));
                                    cuemess = cuemess + key + ":" + valu + ";";
                                } while (c.moveToNext());
                            }
                            PrintWriter serv_out = new PrintWriter(socket.getOutputStream(), true);
                            serv_out.println(cuemess);
                            serv_out.flush();
                        }
                    }
                else if(str[0].equalsIgnoreCase("delete")){
                    int i=delete(uri,str[1],null);
                        PrintWriter serv_out = new PrintWriter(socket.getOutputStream(), true);
                        serv_out.println(i);
                        serv_out.flush();
                    }
                }
            }catch(IOException e){
                Log.e(TAG,"Socket IO exception");
            }
            return null;
        }

    }
    private class ClientTask extends AsyncTask<String, Void, Void>
    {

        @Override
        protected Void doInBackground(String... msgs)
        {
            try{
                String send = msgs[0];
                String[] msg_type=send.split(":");
                if(msg_type[0].equals("Join_Request"))
                {
                    /////code for sending joing message to 5554
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt("11108"));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    out.println(send);
                    out.flush();
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String  str = in.readLine();
                    String[] response=str.split(":");
                    prdcrNode=response[1];
                    sucrNode=response[2];
                    prdcrHash=genHash(String.valueOf(Integer.parseInt(response[1])/2));
                    if(nodeHash.compareTo(prdcrHash)<0){
                        head=1;
                    }
                    sucrHash=genHash(String.valueOf(Integer.parseInt(response[1])/2));
                    String update = "Join_Update" + ":" + "sucsr" + ":" + myPort;
                    try {
                        Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(sucrNode));
                        PrintWriter out1 = new PrintWriter(socket1.getOutputStream(), true);
                        out1.println(update);
                        out1.flush();
                    }
                    catch(Exception e){
                        Log.e(TAG, "Unknown exception");
                        e.printStackTrace();
                    }
                    String update1 = "Join_Update" + ":" + "prdcr" + ":" + myPort;
                    try{
                        Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(prdcrNode));
                        PrintWriter out2 = new PrintWriter(socket2.getOutputStream(), true);
                        out2.println(update1);
                        out2.flush();
                    }catch(Exception e){
                        Log.e(TAG, "Unknown exception");
                        e.printStackTrace();
                    }
                }

            } catch(UnknownHostException e){
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch(IOException e){
                Log.e(TAG, "ClientTask insert IOException");
                e.printStackTrace();
            } catch(Exception e){
                Log.e(TAG, "Unknown exception");
                e.printStackTrace();
            }
           return null;
        }

    }

}
