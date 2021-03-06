package me.sachinbansal.hadoop.servlets;

import java.io.IOException;
import java.util.Vector;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.eclipse.jetty.websocket.api.Session;

import me.sachinbansal.hadoop.models.ChatroomMessage;
import me.sachinbansal.hadoop.server.JavaChatroom;

/**
 * This class represents a simple chatroom. Users are identified only by
 * their IP address.
 */
public class Chatroom {

	/**
	 * This is the list of messages. They only persist for as long as the server 
	 * is running.
	 */
	Vector<ChatroomMessage> messages = new Vector<ChatroomMessage>();

	/**
	 * These are the participants in the chatroom, identified by their websocket
	 * session. In a real application this would be abstracted out more, and they
	 * could have handles.
	 */
	Vector<Session> participants = new Vector<Session>();

	/**
	 * The constructor. Initializes the keepalive calls that will send messages to
	 * the chat participants every 15 seconds to keep the sessions alive.
	 */
	public Chatroom() {
		ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();

		ses.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				for (Session participant : JavaChatroom.getChatroom().participants) {
					try {
						participant.getRemote().sendString("keep-alive");
					} catch (IOException ex) {
						JavaChatroom.getChatroom().participants.remove(participant);
						System.out.println("Websocket Error " + ex.getMessage());
					}
				}
			}
		}, 15, 15, TimeUnit.SECONDS);
	}

	/**
	 * Adds a new chatroom message and broadcasts it to the chatroom
	 * participants.
	 * 
	 * @param crm The ChatroomMessage to add. 
	 */
	public void addMessage(ChatroomMessage crm) {
		this.messages.add(crm);
		try {
			crm.save();
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Vector<Session> sessionsToRemove = new Vector<Session>();
		for (Session participant : this.participants) {
			if (participant.isOpen()) {
				try {
					participant.getRemote().sendString(crm.print());
				} catch (IOException ex) {
					this.participants.remove(participant);
					System.out.println("Websocket Error " + ex.getMessage());
				}
			} else {
				sessionsToRemove.add(participant);
			}
		}
		for(Session participant : sessionsToRemove) {
			this.participants.remove(participant);
		}
	}

	/**
	 * Adds a new participant to the broadcast list.
	 * 
	 * @param participant The Session to add to the participant list. 
	 */
	public void addParticipant(Session participant) {
		this.participants.add(participant);
	}

	/**
	 * Returns the last "count" messages from the conversation.
	 * 
	 * @param count The number of messages to print.
	 * @return 
	 */
	public String print(int count) {
		StringBuffer sb = new StringBuffer();
		if (this.messages.size() < count) {
			for(int i = 0; i < this.messages.size(); i++) {
				ChatroomMessage crm = this.messages.get(i);
				sb.append(crm.print());
			}
		} else {
			for(int i = this.messages.size() - count; i < this.messages.size(); i++) {
				ChatroomMessage crm = this.messages.get(i);
				sb.append(crm.print());
			}
		}
		return sb.toString();
	}
}