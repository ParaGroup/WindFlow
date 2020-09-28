package com.server.CustomServer;

import java.nio.ByteBuffer;

// KeyAttach class
public class KeyAttach
{
    public ByteBuffer response;
    public int clientOp;
    public int idClient;

    // Constructor
    public KeyAttach() {
        response = null;
    }
}
