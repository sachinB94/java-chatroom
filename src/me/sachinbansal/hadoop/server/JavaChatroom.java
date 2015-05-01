package me.sachinbansal.hadoop.server;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;

import me.sachinbansal.hadoop.models.ChatroomMessage;
import me.sachinbansal.hadoop.servlets.Chatroom;
import me.sachinbansal.hadoop.servlets.ChatroomSocket;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.server.WebSocketHandler;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;

import com.mongodb.MongoException;

/**
 * A simple chatroom application to demonstrate websockets.
 *
 * It can be run from the home directory like this:
 *
 * java -jar dist/JavaWebSockets.jar -p 9876
 *
 * where the -p flag changes the default port, which is 8054.
 */
public class JavaChatroom {

	/**
	 * The port to listen on.
	 */
	private int port = 8054;

	/**
	 * The embedded Jetty server we will use.
	 */
	private Server jetty = null;

	/**
	 * The (single) chatroom for this server.
	 */
	private static Chatroom chatroom = new Chatroom();

	/**
	 * @param args the command line arguments
	 * @throws UnknownHostException 
	 */
	public static void main(String[] args) throws UnknownHostException {

		// Create the object
		JavaChatroom jws = new JavaChatroom();

		for (int i = 0; i < args.length; i++) {
			if (args[i].equalsIgnoreCase("-p")) {
				try {
					jws.setPort(Integer.parseInt(args[i + 1]));
				} catch (Exception e) {
					// power on
				}
			}
		}

		// start the server
		jws.startServer();
		jws.createMongoConnection();
		
	}

	/**
	 * Sets the port
	 * 
	 * @param port 
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * Create a connection to MongoDB
	 */
	public void createMongoConnection() {
		try {
			new ChatroomMessage().createMongoConnection();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}	
	
	/**
	 * Starts the server at the specified port.
	 * 
	 * There are two different handlers used, one for the chatroom itself, which operates
	 * through endpoint /chat/, and one for the web app, which is at the webroot and lives 
	 * in /www.
	 */
	public void startServer() {
		System.out.println("Starting server on port " + this.port + "...");
		this.jetty = new org.eclipse.jetty.server.Server(this.port);

		// set up the web socket handler
		ContextHandler contextHandler = new ContextHandler();
		contextHandler.setContextPath("/chat");
		contextHandler.setHandler(new WebSocketHandler() {

			@Override
			public void configure(WebSocketServletFactory factory) {
				factory.setCreator(new WebSocketCreator() {
					@Override
					public Object createWebSocket(ServletUpgradeRequest req, ServletUpgradeResponse resp) {
						
						String query = req.getRequestURI().toString();
						if ((query == null) || (query.length() <= 0)) {
							try {
								resp.sendForbidden("Unspecified query");
							} catch (IOException e) {
								e.printStackTrace();
							}

							return null;
						}

						return new ChatroomSocket();
					}
				});
			}

		});

		// set up the resource handler for the static files
		ResourceHandler resourceHandler = new ResourceHandler();
		resourceHandler.setDirectoriesListed(true);
		resourceHandler.setWelcomeFiles(new String[]{"views/index.html"});
		resourceHandler.setResourceBase("WebContent/public" + File.separator);

		HandlerList handlers = new HandlerList();
		handlers.setHandlers(new Handler[]{contextHandler, resourceHandler});

		this.jetty.setHandler(handlers);

		try {
			this.jetty.start();
		} catch (Exception e) {

		}
		System.out.println("Started.");
	}

	/**
	 * Returns the chatroom, of which there is only one in this simple application.
	 * @return 
	 */
	public static Chatroom getChatroom() {
		return JavaChatroom.chatroom;
	}
}