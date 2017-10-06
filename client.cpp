// Client side C/C++ program to collect wifi packets captured
#include <stdio.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

using namespace::std;

#define PORT 12345
#define BUFFER_MAX_SIZE 1024
int main(int argc, char const *argv[])
{
    struct sockaddr_in address;
    int sock = 0, valread;
    struct sockaddr_in serv_addr;
    // char *hello = "Hello from client";
    // char buffer[BUFFER_MAX_SIZE] = {0};

    size_t buffer_size = BUFFER_MAX_SIZE;
    char *buffer = (char *) malloc(BUFFER_MAX_SIZE * sizeof(char));
    
    if ((sock = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        printf("\n Socket creation error \n");
        return -1;
    }
  
    memset((char*)&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = PF_INET;
    serv_addr.sin_port = htons(PORT);
     
    // Convert IPv4 and IPv6 addresses from text to binary form
    if(inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr) <= 0) {
        printf("\nInvalid address/ Address not supported \n");
        return -1;
    }

    // if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
    //     printf("\nConnection Failed \n");
    //     return -1;
    // }
    int num_of_bytes_read;
    while (num_of_bytes_read = getline(&buffer, &buffer_size, stdin)) {        
    	sendto(sock, buffer, num_of_bytes_read, 0, (struct sockaddr *)&serv_addr, sizeof(serv_addr));	
    }
    
    // printf("Hello message sent\n");
    // valread = read( sock , buffer, 1024);
    // printf("%s\n",buffer );
    return 0;
}
