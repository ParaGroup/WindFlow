#!/bin/bash

for ((i=0; i<100; i++)); do
  # ./c_client localhost &
  java Stub_Client > ./Out/"out$i".log &
  sleep 0.1
done
