/**************************************************************************************
 *  Copyright (c) 2020- Gabriele Mencagli and Fausto Frasca
 *  
 *  This file is part of WindFlow.
 *  
 *  WindFlow is free software dual licensed under the GNU LGPL or MIT License.
 *  You can redistribute it and/or modify it under the terms of the
 *    * GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License, or
 *      (at your option) any later version
 *    OR
 *    * MIT License: https://github.com/ParaGroup/WindFlow/blob/vers3.x/LICENSE.MIT
 *  
 *  WindFlow is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *  You should have received a copy of the GNU Lesser General Public License and
 *  the MIT License along with WindFlow. If not, see <http://www.gnu.org/licenses/>
 *  and <http://opensource.org/licenses/MIT/>.
 **************************************************************************************
 */

package com.server.CustomServer;

import com.google.gson.Gson;
import java.util.Set;
import java.io.Reader;
import java.util.Iterator;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.*;
import java.net.InetSocketAddress;
import org.apache.commons.logging.Log;
import com.server.ServerState.ServerState;
import org.apache.commons.logging.LogFactory;

// ServerWF class
public class ServerWF implements Runnable
{
    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_BLACK = "\u001B[30m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_WHITE = "\u001B[37m";
    public static final String BLACK_BOLD = "\033[1;30m";  // BLACK
    public static final String RED_BOLD = "\033[1;31m";    // RED
    public static final String GREEN_BOLD = "\033[1;32m";  // GREEN
    public static final String YELLOW_BOLD = "\033[1;33m"; // YELLOW
    public static final String BLUE_BOLD = "\033[1;34m";   // BLUE
    public static final String PURPLE_BOLD = "\033[1;35m"; // PURPLE
    public static final String CYAN_BOLD = "\033[1;36m";   // CYAN
    public static final String WHITE_BOLD = "\033[1;37m";  // WHITE

    private static int TIMEOUT_SELECTOR;
    private static int THREAD_MAIN_SLEEP = 200; //sleep time when throw RejectedExecutionException
    private String serverWF = ANSI_YELLOW+"[Server WF] "+ ANSI_RESET;
    private Selector selector;
    private ServerState serverState;
    private ThreadPoolExecutor threadPool;
    private int idClient = 0;
    private boolean isInterrupted = false;
    private ServerSocketChannel serverSocketChannel;
    Log log = LogFactory.getLog(ServerWF.class);

    // Constructor
    public ServerWF()
    {
        this.serverState = ServerState.getInstance();
    }

    // method for initialize the server
    public void configServer() throws IOException
    {
        ServerConfig serverConfig = null;
        Gson gson = new Gson();
        log.info(serverWF+"Reading server configuration");
        try(Reader reader = Files.newBufferedReader(Paths.get("./src/main/java/com/server/CustomServer/Configuration/config.json"))) {
            serverConfig = gson.fromJson(reader,ServerConfig.class);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        this.TIMEOUT_SELECTOR = serverConfig.TIMEOUT_SELECTOR;
        serverSocketChannel = ServerSocketChannel.open();
        ServerSocket serverSocket = serverSocketChannel.socket();
        serverSocket.bind(new InetSocketAddress(serverConfig.PORT));
        log.info(serverWF+"Create new socket: "+YELLOW_BOLD+"[PORT] "+serverConfig.PORT+ANSI_RESET);
        serverSocketChannel.configureBlocking(false);
        threadPool = new ThreadPoolExecutor(serverConfig.CORE_POOL_SIZE,serverConfig.MAX_POOL_SIZE,serverConfig.KEEPALIVETIME, TimeUnit.SECONDS,new ArrayBlockingQueue<Runnable>(serverConfig.QUEUE_SIZE));
        threadPool.prestartAllCoreThreads();
        log.info(serverWF+"Thread Pool:\t"+GREEN_BOLD+"RUNNING"+ANSI_RESET);
        selector = Selector.open();
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        log.info(serverWF+"Custom server:\t"+GREEN_BOLD+"ONLINE"+ANSI_RESET);
    }

    // run method
    public void run()
    {
        while (!isInterrupted || selector.keys().size()!=0) { // exit only if isInterrupted is true and selector.keys.size==0
            try {
                selector.select(TIMEOUT_SELECTOR);
            }
            catch (IOException e) {
                e.printStackTrace();
            }
            Set<SelectionKey> readyKeys = selector.selectedKeys();
            Iterator<SelectionKey> iterator = readyKeys.iterator();
            while (iterator.hasNext()) {
                SelectionKey key = iterator.next();
                try {
                    if (key.isAcceptable()) {
                        this.Acceptable(key);
                        iterator.remove();
                    }
                    else if (key.isReadable()) {
                        key.interestOps(0);
                        this.Readable(key);
                        iterator.remove();
                    }
                    else if (key.isWritable()) {
                        this.Writable(key);
                        iterator.remove();
                    }
                }
                catch (IOException e) { // handle closed client's key
                    KeyAttach keyAttach = (KeyAttach) key.attachment();
                    if (keyAttach.clientOp != ClientOperation.END_APP) {
                        serverState.interruptedApplication(keyAttach.idClient);
                    }
                    key.cancel();
                    try {
                        log.debug(serverWF+"Close:\t"+((SocketChannel) key.channel()).getRemoteAddress());
                        key.channel().close();
                    }
                    catch (IOException ioException) {
                        ioException.printStackTrace();
                    }
                }
            }
        }
        log.info(serverWF+"Thread Pool:\t"+CYAN_BOLD+"SHUTDOWN"+ANSI_RESET);
        threadPool.shutdownNow();
        log.info(serverWF+"Thread Pool:\t"+RED_BOLD+"CLOSED"+ANSI_RESET);
        log.info(serverWF+"Custom server:\t"+RED_BOLD+"CLOSED"+ANSI_RESET);
    }

    // method for accept new connections
    private void Acceptable(SelectionKey key) throws IOException
    {
        ServerSocketChannel server = (ServerSocketChannel) key.channel();
        SocketChannel client = server.accept();
        log.debug(serverWF+"New:\t"+client.getRemoteAddress());
        client.configureBlocking(false);
        SelectionKey key1 = client.register(selector,SelectionKey.OP_READ);
        key1.attach(new KeyAttach());

    }

    // method for read from readable connections
    private void Readable(SelectionKey key) throws IOException
    {
        SocketChannel client = (SocketChannel) key.channel();
        ByteBuffer buffer = ByteBuffer.allocate(4);
        boolean sendTask = false;
        int read;
        log.debug(serverWF+"Read:\t"+client.getRemoteAddress().toString());
        read = client.read(buffer);
        if (read == -1) {
            throw new IOException("Closed channel");
        }
        else {
            buffer.flip();
            int clientOp = buffer.getInt();
            if (clientOp == ClientOperation.NEW_APP) {
                do {
                    try {
                        threadPool.execute(new WorkerWF(key,idClient,clientOp));
                        sendTask = true;
                    }
                    catch (RejectedExecutionException e) {
                        try {
                            log.debug(serverWF+"\tRejectedExecutionException "+idClient);
                            Thread.sleep(THREAD_MAIN_SLEEP);
                        } 
                        catch (InterruptedException interruptedException) {
                            interruptedException.printStackTrace();
                        }
                    }
                } while (!sendTask);
                idClient++;
            }
            else {
                do {
                    try {
                        threadPool.execute(new WorkerWF(key,clientOp));
                        sendTask = true;
                    }
                    catch (RejectedExecutionException e){
                        try {
                            log.debug(serverWF+"\tRejectedExecutionException");
                            Thread.sleep(THREAD_MAIN_SLEEP);
                        }
                        catch (InterruptedException interruptedException) {
                            interruptedException.printStackTrace();
                        }
                    }
                } while (!sendTask);
            }
        }
    }

    // method for write to writable connections
    private void Writable(SelectionKey key) throws IOException
    {
        SocketChannel client = (SocketChannel) key.channel();
        KeyAttach keyAttach = (KeyAttach) key.attachment();
        ByteBuffer response = keyAttach.response;
        response.flip();
        while (response.hasRemaining()) {
            log.debug(serverWF+"Write:\t"+client.getRemoteAddress());
            client.write(response);
        }
        if (this.isInterrupted) { // cancel key if isInterrupted is true
            throw new IOException();
        }
        else {
            key.interestOps(SelectionKey.OP_READ);
        }
    }

    // method for soft server shutdown
    public void interrupt()
    {
        log.info(serverWF+"Custom server:\t"+CYAN_BOLD+"SHUTDOWN"+ANSI_RESET);
        this.isInterrupted=true;
        try {
            serverSocketChannel.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
