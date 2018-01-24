package org.qcri.rheem.spark.debug;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.Socket;

/**
 * Created by bertty on 01-06-17.
 */
public class SocketSerializable<Type> implements Serializable {
   private Socket      socket;
   private PrintWriter out;
   private String      name;
   private String      my_ip;
   private String      host;
   private int         port;
   private String      prefix;

   public SocketSerializable(String name, String host, int port){
       this.name = name;
       this.host = host;
       this.port = port;
   }

   private void create(){
       try {
           this.socket = new Socket(this.host, this.port);
           this.out    = new PrintWriter(this.socket.getOutputStream());
           this.my_ip  = InetAddress.getLocalHost().getHostAddress();
           this.prefix = this.name + " " + this.my_ip + " ";
       } catch (IOException e) {
           e.printStackTrace();
       }
   }

   public void sendMessage(Type object){
       if(this.socket == null){
           create();
       }
       out.println(prefix + object);
   }



}
