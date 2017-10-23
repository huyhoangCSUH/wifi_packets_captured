// Server side: receiving data from the clients and input data into Vertica
#include <stdio.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <assert.h>
#include <sstream>
// Standard ODBC headers
#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <fstream>
#include <iostream>
// For libpq
#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>
#include <ctime>

using namespace::std;

#define PORT 12345
#define BUFFER_MAX_SIZE 1024

void StringPrepareForPostgres(string& raw_string, int num_of_cols, int last_col_max_char);
bool notSuccess(SQLRETURN ret) {
    return (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO);
}
static void exit_nicely(PGconn *conn) {
    PQfinish(conn);
    exit(1);
}

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
    const char *conninfo = "dbname = postgres";
    PGconn     *conn;
    PGresult   *ret;
    /* Make a connection to the database */
    conn = PQconnectdb(conninfo);

    /* Check to see that the backend connection was successfully made */
    if (PQstatus(conn) != CONNECTION_OK)
    {
        fprintf(stderr, "Connection to database failed: %s",
                PQerrorMessage(conn));
        exit_nicely(conn);
    }

    // Start listening and receiving data
    printf("Start listening: %s\n", buffer);
    string statement;
    int rows_added = 0;
    ofstream fout("data.csv");
    clock_t begin = clock();
    clock_t end = clock();
    double elapsed_secs = double(end - begin) / CLOCKS_PER_SEC;
    double frequency = 10; //frequency to add data
    while(1) {
        bytes_recv = recvfrom(server_fd, buffer, BUFFER_MAX_SIZE, 0, (struct sockaddr *)&remote_addr, &addrlen);        
        
        string copyString;
        if (bytes_recv > 0) {
            string buffer_str(buffer);
            StringPrepareForPostgres(buffer_str, 6, 30);
            fout << buffer_str;
            end = clock();
            //rows_added++;
            elapsed_secs = double(end - begin) / CLOCKS_PER_SEC;

            if (elapsed_secs >= frequency) {                
                fout.close();
                printf("Prepare to import 10000 rows!\n");
                if(rename("data.csv", "archive/data_to_import.csv"));
                
                cout << copyString << endl;
                ret = PQexec(conn, "COPY network_log "
                    "FROM '/home/postgres/wifi_packets_captured/archive/data_to_import.csv' CSV");
                
                if (PQresultStatus(ret) != PGRES_TUPLES_OK) {
                    fprintf(stderr, "COPY failed: %s", PQerrorMessage(conn));
                    PQclear(ret);
                    exit_nicely(conn);
                } else {
                    if (remove("archive/data_to_import.csv") != 0) {
                        printf("Error deleting file!\n");
                    } else {
                        printf("File deleted sucessfully!\n");
                    }
                }                                           
                PQclear(ret);
                rows_added = 0;
                fout.open("data.csv");
                begin = clock();
            }
        }
    }   
    return 0;
}

void StringPrepareForPostgres(string& raw_string, int num_of_cols, int last_col_max_char) {    
    // This function is to trim the last column to the size of table's column        
    string output_string;    
    int last_pos_of_comma = 0;        
    int comma_count = 0;
    for (int i = 0; i < raw_string.size(); ++i) {        
        if (raw_string.at(i) == ',') {
            comma_count++;
            //cout << comma_count << endl;
            if (comma_count == num_of_cols - 1) {
                last_pos_of_comma = i;
                break;
            }
        }
    }
    int last_col_char_count = 0; 
    int i = last_pos_of_comma + 1;
    while (last_col_char_count < last_col_max_char && raw_string.at(i) != '\0') {
        //if (raw_string.at(i) == ',' || raw_string.at(i) == '\"' )
        if (raw_string.at(i) == ',')    
            raw_string.erase(i, 1); 
        i++;
        last_col_char_count++;
    }
    raw_string.erase(i, 500);  
    raw_string += "\"\n";

};