# wifi_packets_captured
client-server programs to deliver wifi packets captured by tshark

To compile  server_postgres.cpp:
```
g++ -I/usr/include/postgresql -L/usr/lib server_postgres.cpp -o server_pg -lpq
```

To compile server_vertica.cpp:
```
g++ server_vertica.cpp -o server_vertica -lodbc
```
