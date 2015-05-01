package me.sachinbansal.hadoop.servlets;

import java.io.IOException;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;

import me.sachinbansal.hadoop.models.ChatroomMessage;
import me.sachinbansal.hadoop.server.JavaChatroom;

/**
 * The socket that users use when connected to the chatroom.
 * 
 */
public class ChatroomSocket extends WebSocketAdapter{

	/**
	 * The Session of this socket.
	 */
	private Session session;

	//@OnWebSocketConnect
	@Override
	/**
	 * Adds the session to the chatroom participants list, and sends back to the 
	 * user the last three messages in the conversation.
	 */
	public void onWebSocketConnect(Session session) {
		System.out.println("+++ Websocket Connect from " + session.getRemoteAddress().getAddress()); 
		this.session = session;
		JavaChatroom.getChatroom().addParticipant(session);
		try {
			this.session.getRemote().sendString(JavaChatroom.getChatroom().print(3));
		} catch (IOException ex) {
			System.out.println("+++ Websocket Error " + ex.getMessage());
		}
	}

	//@OnWebSocketMessage
	@Override
	public void onWebSocketBinary(byte[] bytes, int x, int y) {
		// not used
	}

	// @OnWebSocketMessage
	@Override
	/**
	 * Adds the message from the user to the chatroom conversation.
	 */
	public void onWebSocketText(String message) {
		if(message != null && !message.equals("keep-alive")) {
			ChatroomMessage crm = new ChatroomMessage(session.getRemoteAddress().getAddress().toString().substring(1), message);  
			JavaChatroom.getChatroom().addMessage(crm);
		}
	}

//	@Override
	public void onError(Throwable cause){
		System.out.println("+++ Websocket Error " + cause.getMessage());
	}

	@Override
	public void onWebSocketClose(int statusCode, String reason) {
		System.out.println("+++ Websocket Close from " + session.getRemoteAddress().getAddress()); 
	}
}