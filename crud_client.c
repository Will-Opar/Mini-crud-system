////////////////////////////////////////////////////////////////////////////////
//
//  File          : crud_client.c
//  Description   : This is the client side of the CRUD communication protocol.
//
//   Author       : Patrick McDaniel
//  Last Modified : Thu Oct 30 06:59:59 EDT 2014
//

// Include Files

// Project Include Files
#include <crud_network.h>
#include <cmpsc311_log.h>
#include <cmpsc311_util.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <errno.h>
#include <unistd.h>
// Global variables
int            crud_network_shutdown = 0; // Flag indicating shutdown
unsigned char *crud_network_address = NULL; // Address of CRUD server 
unsigned short crud_network_port = 0; // Port of CRUD server
int sock;
//
// Prototype Functions
int get_len(CrudResponse cr);

//Functions

///////////////////////////////////
//
// Function: get_req
//
// Description: returns the request of a CrudResponse.
// Input : CrudReponse you want the request for
// Output : request of the CrudResponse input
//

int8_t get_req(CrudResponse cr)
{
	int8_t ret;
	ret = (cr >> 28) & 0xf;
	return ret;
}

//////////////////////////////////
//
// Function: crud_send
//
// Description: Sends data over to the server
// Input: CrudRequest and buffer you want to send
// Output: none
//

void crud_send( CrudRequest op, void *buf)
{

	// Declare Variables
	struct sockaddr_in caddr;
	int  len , buf_len = 0, place = 0 ;
	int req = get_req(op);

	// Connect to the network if not connected
	if(crud_network_shutdown == 0)
	{
		caddr.sin_family = AF_INET;
		caddr.sin_port = htons(CRUD_DEFAULT_PORT);

		if ( inet_aton(CRUD_DEFAULT_IP , &caddr.sin_addr) == 0)
		{
			printf("inet_aton error");
			exit(1);
		}

		sock = socket(PF_INET, SOCK_STREAM, 0);

		if(sock == -1)
		{
			printf("Sockect error \n" );
			exit(1);
		}

		if( connect( sock, (const struct sockaddr *)&caddr, sizeof(struct sockaddr)) == -1)
		{
			printf("Connect error \n"  );
			exit(1);

		}
	
		crud_network_shutdown = 1;
	}

	// get length from CrudRequest
	len = get_len(op);

	// Convert Crudrequest to network order bytes
	op = htonll64(op);	

	// Write request to server 
	buf_len = write(sock,&op, sizeof(op));
	
	//Check if request it sent
	if(buf_len < sizeof(op))
	{
		printf("not everything");
		exit(1);
	}
	if(buf_len < 0)
	{
		printf("Didnt send request #1 \n"); 
		exit(1);

	}
	
	// Restart buf_len
	buf_len =0;
		
	// Send buffer if needed
	if( req == CRUD_CREATE || req == CRUD_UPDATE)
	{
		while( buf_len != len)
		{
			buf_len = write(sock,buf + place,len);
	
			if(buf_len < 0)
			{
				printf("did not write buffer \n"  );
				exit(1) ;
			}
			
			if(buf_len != len)
			{
				len = len - buf_len;
				place += buf_len;
			}
		}	
	}
}

////////////////////////////////////////
//
// Function: crud_receive 
//
// Description: receives data from the server
// Input: buffer pointer to hold all the data you get from the server
// Output: CrudResponse response from the server
//

CrudResponse crud_receive( void *buf)
{
	// Declare Variable
	int req, len, buf_len = 0, place = 0 ;
	CrudResponse ret;
	
	// Check connection
	if(crud_network_shutdown ==0)
	{

		printf("Error connect \n");
		exit(1);
	}

	// Receive the Response from the server
	buf_len = read(sock,&ret, sizeof(ret));
	
	// Check if we receive the Response
	if(buf_len < 0)
	{
		printf("Didnt read request #1 \n"); 
		exit(1);
	
	}	
	
	// Restart buf_len
	buf_len = 0;

	// Convert Response to host order
	ret = ntohll64(ret);
	// get the length and request from the Response
	len = get_len(ret);
	req = get_req(ret);
	
	// Read a buffer if needed
	if(req == CRUD_READ)
	{
		
		while( buf_len != len)
		{
			buf_len = read(sock,buf+place, len);
			
			if(buf_len < 0 )
			{
				printf("did not read buffer \n"  );
				exit(1);
			}

			if(buf_len != len)
			{
				len = len - buf_len;
				place += buf_len;
			}
		}
	}
	
	// Close the socket when requested
	if(req == CRUD_CLOSE)
	{
		close(sock);
	}

	// return the Response
	return ret;
	
}


////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_client_operation
// Description  : This the client operation that sends a request to the CRUD
//                server.   It will:
//
//                1) if INIT make a connection to the server
//                2) send any request to the server, returning results
//                3) if CLOSE, will close the connection
//
// Inputs       : op - the request opcode for the command
//                buf - the block to be read/written from (READ/WRITE)
// Outputs      : the response structure encoded as needed

CrudResponse crud_client_operation(CrudRequest op, void *buf) {
	crud_send(op, buf);
	op = crud_receive(buf);
	return op;
}

