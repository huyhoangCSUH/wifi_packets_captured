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
    /*
    *
    *   Data import part
    *
    */
    SQLRETURN ret;   // Stores return value from ODBC API calls
    SQLHENV hdlEnv;  // Handle for the SQL environment object
    // Allocate an a SQL environment object
    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &hdlEnv); 
    if(!SQL_SUCCEEDED(ret)) {
        printf("Could not allocate a handle.\n");
        exit(EXIT_FAILURE);
    } else {
        printf("Allocated an environment handle.\n");
    }
    // Set the ODBC version we are going to use to 
    // 3.
    ret = SQLSetEnvAttr(hdlEnv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER) SQL_OV_ODBC3, SQL_IS_UINTEGER);
    if(!SQL_SUCCEEDED(ret)) {
         printf("Could not set application version to ODBC 3.\n");
         exit(EXIT_FAILURE);
    } else {
         printf("Set application version to ODBC 3.\n");
    }
    // Allocate a database handle.
    SQLHDBC hdlDbc;
     ret = SQLAllocHandle(SQL_HANDLE_DBC, hdlEnv, &hdlDbc); 
     if(!SQL_SUCCEEDED(ret)) {
          printf("Could not allocate database handle.\n");
          exit(EXIT_FAILURE);
     } else {
          printf("Allocated Database handle.\n");
     }
    // Connect to the database using 
    // SQL Connect
    printf("Connecting to database.\n");
    const char *dsnName = "mydsn";
    const char* userID = "vertica";
    const char* passwd = "12512Marlive";
    ret = SQLConnect(hdlDbc, (SQLCHAR*)dsnName, SQL_NTS,(SQLCHAR*)userID,SQL_NTS,
        (SQLCHAR*)passwd, SQL_NTS);
    if(!SQL_SUCCEEDED(ret)) {
        printf("Could not connect to database.\n");
        exit(EXIT_FAILURE);
    } else {
        printf("Connected to the database.\n");
    }
    // Disable autocommit to boost inserting speed
    printf("Disabling autocommit.\n");
    ret = SQLSetConnectAttr(hdlDbc, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, SQL_NTS);
    if(!SQL_SUCCEEDED(ret)) {
        printf("Could not disable autocommit.\n");
        exit(EXIT_FAILURE);
    }
    // This part is for executing SQL queries
    // Set up a statement handle
    SQLHSTMT hdlStmt;
    SQLAllocHandle(SQL_HANDLE_STMT, hdlDbc, &hdlStmt);
    assert(SQL_SUCCEEDED(ret)); 
    
    // Start listening and receiving data
    printf("Start listening: %s\n", buffer);
    string statement;
    int rows_added = 0;
    while(1) {
        bytes_recv = recvfrom(server_fd, buffer, BUFFER_MAX_SIZE, 0, (struct sockaddr *)&remote_addr, &addrlen);        
        statement = "INSERT INTO network_log VALUES ("
        if (bytes_recv > 0) {
            buffer[bytes_recv] = 0;
            //printf("%s\n", buffer);
            statement.append(buffer);
            statement.append(")");
            ret = SQLExecDirect(hstmt, (SQLTCHAR*) &statement, SQL_NTS);
            if(!SQL_SUCCEEDED(ret)) { 
                printf("A row rejected!\n");
            } else {
                rows_added++;
                // Commit manually after 1000 rows inserted
                if (rows_added >= 1000) {
                    ret = SQLEndTran(SQL_HANDLE_DBC, hdlDbc, SQL_COMMIT);
                    if(!SQL_SUCCEEDED(ret)) {
                        printf("Could not commit transaction\n");                        
                    }  else {
                        printf("Committed transaction\n");
                    }
                }
            }

        }
    }
   
    return 0;
}