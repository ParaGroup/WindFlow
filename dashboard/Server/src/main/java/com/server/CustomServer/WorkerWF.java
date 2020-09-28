package com.server.CustomServer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import com.server.ServerState.ServerState;

// WorkerWF class
public class WorkerWF implements Runnable
{
    private SelectionKey key;
    private Selector selector;
    private int idClient = -1;
    private int clientOp;
    private ServerState serverState;
    private String serverWF = "[Server WF] ";

    // Constructor I
    public WorkerWF(SelectionKey key, Selector selector, int idClient, int clientOp)
    {
        this.key = key;
        this.selector = selector;
        this.idClient = idClient;
        this.clientOp = clientOp;
        this.serverState = ServerState.getInstance();
    }

    // Constructor II
    public WorkerWF(SelectionKey key, Selector selector, int clientOp)
    {
        this.key = key;
        this.selector = selector;
        this.clientOp = clientOp;
        this.serverState = ServerState.getInstance();
    }

    // run method
    @Override public void run()
    {
        try {
            if (clientOp == ClientOperation.NEW_APP) {
                newApp();
                sendMessage();
            }
            else {
                newReport_endApp(clientOp);
                sendMessage();
            }
        }
        catch (IOException e) {
            registerKeyToSelector();
            return;
        }

    }

    // newApp method
    private void newApp() throws IOException
    {
        SocketChannel client = (SocketChannel) key.channel();
        ByteBuffer buffer = ByteBuffer.allocate(4);
        String remoteAddress = client.getRemoteAddress().toString();
        remoteAddress = remoteAddress.substring(1,remoteAddress.indexOf(":"));
        client.read(buffer);
        buffer.flip();
        int len = buffer.getInt();
        buffer = ByteBuffer.allocate(len);
        byte[] payload = new byte[len];
        int read=0, len_read=0;
        while (len-len_read>0) {
            read=client.read(buffer);
            len_read+=read;
        }
        buffer.flip();
        buffer.get(payload);
        String strPayload = new String(payload,0,len);
        serverState.addApplication(idClient,strPayload, remoteAddress);

    }

    // newReport_endApp method
    private void newReport_endApp(int clientOp) throws IOException
    {
        SocketChannel client = (SocketChannel) key.channel();
        ByteBuffer buffer = ByteBuffer.allocate(8);
        client.read(buffer);
        buffer.flip();
        idClient = buffer.getInt();
        int len = buffer.getInt();
        buffer = ByteBuffer.allocate(len);
        byte[] payload = new byte[len];
        int read=0, len_read=0;
        while (len-len_read>0) {
            read=client.read(buffer);
            len_read+=read;
        }
        buffer.flip();
        buffer.get(payload);
        String strPayload = new String(payload,0,len);
        String msg = clientOp == 1? "REPORT" : "END";
        if(clientOp == 1) {
            serverState.addNewReport(idClient,strPayload);
        }
        else {
            serverState.endApplication(idClient, strPayload);
        }
    }

    // sendMessage method
    private void sendMessage()
    {
        KeyAttach keyAttach = (KeyAttach) key.attachment();
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putInt(0);
        buffer.putInt(idClient);
        keyAttach.response = buffer;
        keyAttach.clientOp = clientOp;
        keyAttach.idClient = idClient;
        registerKeyToSelector(keyAttach);
    }

    // registerKeyToSelector method
    private void registerKeyToSelector()
    {
        SocketChannel client = (SocketChannel) key.channel();
        this.key.interestOps(SelectionKey.OP_READ);
        // try {
        // SelectionKey key1 = client.register(this.selector,SelectionKey.OP_READ);
        // } catch (ClosedChannelException e) {
        // e.printStackTrace();
        // }
    }

    // registerKeyToSelector method
    private void registerKeyToSelector(KeyAttach keyAttach)
    {
        SocketChannel client = (SocketChannel) key.channel();
        // try {
        // SelectionKey key1 = client.register(this.selector,SelectionKey.OP_WRITE);
        // key1.attach(keyAttach);
        this.key.attach(keyAttach);
        this.key.interestOps(SelectionKey.OP_WRITE);
        // } catch (ClosedChannelException e) {
        // e.printStackTrace();
        // }
    }
}
