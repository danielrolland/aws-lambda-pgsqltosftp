package com.drolland.pgtosftp;


import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.postgresql.PGConnection;
import org.postgresql.copy.*;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

/**
 * Backups the tables from the database schema and sends it through SFTP
 * @author d.rolland
 */
public class PostgreSQLExport {
	
	// RDS environment variables
	private final String DB_HOST = System.getenv("DB_HOST");
 	private final String DB_PORT = System.getenv("DB_PORT");
 	private final String DB_USER = System.getenv("DB_USER");
 	private final String DB_PASSWD = System.getenv("DB_PASSWD");
 	private final String DB_NAME = System.getenv("DB_NAME");
 	private final String DB_SCHEMA = System.getenv("DB_SCHEMA");
 	private final String DB_URL = "jdbc:postgresql://" + DB_HOST + ":" + DB_PORT + "/" + DB_NAME;
 	private final String QUERY_LIMIT = System.getenv("QUERY_LIMIT");
 	private String TABLE_TYPES = System.getenv("TABLE_TYPES");
 	
 	// SFTP environment variables
 	private final String SFTP_USER = System.getenv("SFTP_USER");
 	private final String SFTP_PASSWD = System.getenv("SFTP_PASSWD");
 	private final String SFTP_PORT = System.getenv("SFTP_PORT");
 	private final String SFTP_HOST = System.getenv("SFTP_HOST");
 	private final String SFTP_FOLDER = System.getenv("SFTP_FOLDER");
	
	private Connection db = null;
	
    public void exportHandler(ScheduledEvent event, Context context) throws SQLException, IOException, JSchException, SftpException {
    	
    	// Connect to the RDS database
    	try {
    		Class.forName("org.postgresql.Driver");
    		db = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWD);
    		System.out.println("Successfully connected to the database " + DB_HOST);
    	} catch (ClassNotFoundException | SQLException ex) {
    		System.out.println(ex);
    	}
    	
    	// Configure table types, get tables and instantiate the copy manager
    	DatabaseMetaData md = db.getMetaData();
    	String[] tableTypes = (TABLE_TYPES == null || TABLE_TYPES.equals("") )? null : TABLE_TYPES.trim().split(",");
    	ResultSet tables = md.getTables(DB_NAME, DB_SCHEMA, null, tableTypes );
    	if	(tables == null ) {
    		System.out.println("No tables found!");
    		return;
    	}
    	System.out.println("Retrieved tables from " + DB_NAME + ", " + DB_SCHEMA);
    	
    	// Set today's date
    	DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    	Date date = new Date();
    	String dateString = dateFormat.format(date);
    	
    	// Configure SFTP connection, connect and go to the target folder
    	JSch jsch = new JSch();
    	Integer sftpPort = Integer.parseInt(SFTP_PORT);
    	Session session = jsch.getSession(SFTP_USER, SFTP_HOST, sftpPort);
    	session.setPassword(SFTP_PASSWD);
    	java.util.Properties config = new java.util.Properties();
        config.put("StrictHostKeyChecking", "no");
        session.setConfig(config);
        System.out.println("Connecting to the SFTP server...");
    	session.connect();
    	System.out.println("Connected to sftp://" + SFTP_HOST);
        Channel channel = session.openChannel("sftp");
        channel.connect();
        System.out.println("SFTP channel opened and connected.");
        ChannelSftp channelSftp = (ChannelSftp) channel;
        channelSftp.cd(SFTP_FOLDER);
    	
        // Loop through tables
    	while(tables.next()) {
    		// Set up table and file info
    		String tableName = tables.getString(3);
    		System.out.println("Current table : " + tableName);
    		String fileName = tableName + "_" + dateString +".csv";
        	
        	// Define the query based on whether or not we have set a limit
        	String theSql;
        	if (QUERY_LIMIT == null || QUERY_LIMIT.equals("")) {
        		theSql = new String("COPY " + DB_SCHEMA + "." + tableName + " TO STDOUT WITH (FORMAT CSV, NULL 'null', HEADER )");
        	}
        	else {
        		theSql = new String("COPY (SELECT * FROM " + DB_SCHEMA + "." + tableName + " LIMIT " + QUERY_LIMIT + ") TO STDOUT WITH (FORMAT CSV, NULL 'null', HEADER )");
        	}
        	
        	// Proceed to table copy and send stream through SFTP
        	try {            	
            	PGCopyInputStream inputStream = new PGCopyInputStream((PGConnection) db, theSql);
            	System.out.println("Number of rows : " + inputStream.getHandledRowCount());
            	channelSftp.put(inputStream, fileName);
            	System.out.println("Successfully uploaded " + fileName + " to " + SFTP_HOST);
            	// If there is an SQL Exception we don't want it to be blocking as the table might just not be in the correct schema
        	} catch (SQLException e) {
        		System.out.println("SQL ERROR : could not backup the table " + tableName + " : " + e);
        	} finally {
        		System.out.println("End of " + tableName + " processing.");
        	}
        	
    	}
        
        // Disconnect properly
        channelSftp.exit();
        System.out.println("SFTP channel exited.");
        channel.disconnect();
        System.out.println("Channel disconnected.");
        session.disconnect();
        System.out.println("Disconnected from host.");
    	
    }
        
}

	
