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

import java.net.*;
import java.util.Arrays;
import java.nio.ByteOrder;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.channels.SocketChannel;

// Class Stub_Client
public class Stub_Client
{
    /**
     * args[0] = "hostname", optional parameter is "locahost"
     */
    public static int PORT = 20207;

    // main method
    public static void main(String[] args) throws UnknownHostException
    {
        String host = InetAddress.getLocalHost().getHostAddress();
        // String host = "131.114.3.249";
        int send = 3;
        if (args.length==1) {
            host = args[0];
        }
        try {
            SocketAddress address = new InetSocketAddress(host, PORT);
            SocketChannel client = null;
            try {
                client = SocketChannel.open(address); // try to connect to the server
            }
            catch (ConnectException e) {
                System.out.println("Server is not reachable...I will re-try later");
                try {
                    Thread.sleep(3000);
                    System.out.println("I am trying to connect again...");
                    client = SocketChannel.open(address);
                    System.out.println();
                }
                catch (ConnectException ek) {
                    System.out.println("Server is not reachable");
                    return;
                }
                catch (InterruptedException ex) {
                    ex.printStackTrace();
                }

            }
            System.out.println("Connection established\n");
            ByteBuffer buffer = new_app();
            writeToServer(buffer,client);
            int idClient = readFromServer(client);
            for (int i=0; i<send; i++) {
                buffer = new_report(idClient,1);
                writeToServer(buffer,client);
                readFromServer(client);
                System.out.println(i);
                Thread.sleep(1000);
            }
            buffer = new_report(idClient,2);
            writeToServer(buffer,client);
            readFromServer(client);

        }
        catch (IOException e) {
            e.printStackTrace();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // method to read from the server
    private static int readFromServer(SocketChannel client) throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate(1024); // crate buffer to store data from server
        buffer.order();
        int read = client.read(buffer); // read the string provided by the server
        buffer.flip();
        if (read == -1) {
            return -1; // check if the connection has been closed
        }
        int ack = buffer.getInt();
        int code = buffer.getInt();
        System.out.println("[SERVER] ack: "+ack+"\tcode: "+code);
        return code;
    }

    // method to write a data to the server
    private static void writeToServer(ByteBuffer buffer, SocketChannel client) throws IOException
    {
        buffer.flip();
        while (buffer.hasRemaining()) { // send data to the server
            client.write(buffer);
        }
    }

    // method to send the new_app message to the server
    private static ByteBuffer new_app()
    {
        byte[] payload = new byte[0];
        try {
            payload = Files.readAllBytes(Paths.get("./test_diagram"));
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        // System.out.println("Payload len "+payload.length);
        ByteBuffer buffer = ByteBuffer.allocate(8+payload.length);
        buffer.putInt(0);
        buffer.putInt(payload.length);
        buffer.put(payload);
        // byte[] result = buffer.array();
        // System.out.println(Arrays.toString(result));
        return buffer;
    }

    // method to send a new_report message to the server
    private static ByteBuffer new_report(int idClient, int op)
    {
        byte[] payload = new byte[0];
        try {
            payload = Files.readAllBytes(Paths.get("./report.json"));
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        // System.out.println("Payload len "+payload.length);
        ByteBuffer buffer = ByteBuffer.allocate(12+payload.length);
        buffer.putInt(op);
        buffer.putInt(idClient);
        buffer.putInt(payload.length);
        buffer.put(payload);
        return buffer;
    }
}
