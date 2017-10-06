// Server side: receiving data from the clients and input data into Vertica
#include <stdio.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/types.h>

using namespace::std;

#define PORT 12345
#define BUFFER_MAX_SIZE 1024

int main(int argc, char const *argv[])
{
    int server_fd, new_socket, valread;
    struct sockaddr_in address;         // Server address
    struct sockaddr_in remote_addr;     // remote address
    int opt = 1;    
    char buffer[BUFFER_MAX_SIZE] = {0};
    int bytes_recv; // Number of bytes received
    socklen_t addrlen = sizeof(remote_addr);

    // Creating socket file descriptor
    if ((server_fd = socket(AF_INET, SOCK_DGRAM, 0)) == 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }
    socklen_t optlen = sizeof(opt);  
    // Forcefully attaching socket to the port 
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEPORT, &opt, optlen)) {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }
    memset((char*)&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons( PORT );
      
    // Forcefully attaching socket to the port 
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }
    // if (listen(server_fd, 3) < 0) {
    //     perror("listen");
    //     exit(EXIT_FAILURE);
    // }
    // if ((new_socket = accept(server_fd, (struct sockaddr *)&address, 
    //                    (socklen_t*)&addrlen))<0) {
    //     perror("accept");
    //     exit(EXIT_FAILURE);
    // }
    while(1) {
        bytes_recv = recvfrom(server_fd, buffer, BUFFER_MAX_SIZE, 0, (struct sockaddr *)&remote_addr, &addrlen);
        // printf("Received %d bytes\n", bytes_recv);
        if (bytes_recv > 0) {
            buffer[bytes_recv] = 0;
            printf("Message received: %s\n", buffer);
        }
    }
    // printf("%s\n",buffer );
    // send(new_socket , hello , strlen(hello) , 0 );
    // printf("Hello message sent\n");
    return 0;
}