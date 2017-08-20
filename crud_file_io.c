////////////////////////////////////////////////////////////////////////////////
//
//  File           : crud_file_io.h
//  Description    : This is the implementation of the standardized IO functions
//                   for used to access the CRUD storage system.
//
//  Author         : Patrick McDaniel
//  Last Modified  : Mon Oct 20 12:38:05 PDT 2014
//

// Includes
#include <malloc.h>
#include <string.h>

// Project Includes
#include <crud_file_io.h>
#include <cmpsc311_log.h>
#include <cmpsc311_util.h>

// Defines
#define CIO_UNIT_TEST_MAX_WRITE_SIZE 1024
#define CRUD_IO_UNIT_TEST_ITERATIONS 10240

// Other definitions

// Type for UNIT test interface
typedef enum {
	CIO_UNIT_TEST_READ   = 0,
	CIO_UNIT_TEST_WRITE  = 1,
	CIO_UNIT_TEST_APPEND = 2,
	CIO_UNIT_TEST_SEEK   = 3,
} CRUD_UNIT_TEST_TYPE;

// File system Static Data
// This the definition of the file table
CrudFileAllocationType crud_file_table[CRUD_MAX_TOTAL_FILES]; // The file handle table
int p_obj, init = 0;
// Pick up these definitions from the unit test of the crud driver
CrudRequest construct_crud_request(CrudOID oid, CRUD_REQUEST_TYPES req,
		uint32_t length, uint8_t flags, uint8_t res);
int deconstruct_crud_request(CrudRequest request, CrudOID *oid,
		CRUD_REQUEST_TYPES *req, uint32_t *length, uint8_t *flags,
		uint8_t *res);

//
// Implementation

CrudResponse crud_client_operation(CrudRequest , void *);
///////////////////////////////////////////////////////////////////////////////
//
// Function   : crud_format
// Description: This function takes the parameters of the crudResponse and converts them
//
// Inputs     : oid - object id, req - request type, len - length of object, flag - Flags, ret - return value
// Outputs    : the crudRequest
//
CrudRequest crud_formati(int32_t oid, int8_t req, int32_t len, int8_t flag, int8_t ret)
{
	CrudRequest retv;
	retv =  ((int64_t)oid << 32);
	retv |= ((req & 0xf) << 28);
	retv |= ((len & 0xffffff) << 4);
	retv |= ((flag & 0xb) << 1);
	retv |= (ret & 0x1);

	return (CrudRequest)retv;
}

///////////////////////////////////////////////////////////////////////////////
//
// Function   : get_oid
// Description: This function takes the CrudResponse and returns the object id
//
// Inputs     : CrudResponse - the Crud Response 
// Outputs    : Object Id - of the Crud Response
//
int32_t get_oid(CrudResponse crud){
	return ((crud >> 32) & 0xffffffff);
}

/////////////////////////////////////////////////////////////////////////////////
//
// Function   : get_len
// Description: This function takes CrudResponse and returns the len
//
// Inputs     : CrudResponse - the Crud Response
// Outputs    : Length of the Crud Response
// 
int32_t get_len(CrudResponse crud)
{
	return ((crud >> 4) & 0xffffff);
}

////////////////////////////////////////////////////////////////////////////////
//
// Function   : get_ret 
// Description: This function takes the CrudResponse and get the return value
//
//  Inputs    : CrudResponse - the Crud Response
//  outputs   : Return value of the Crud Reponse
int8_t get_ret(CrudResponse crud){
	return crud & 1;
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_format
// Description  : This function formats the crud drive, and adds the file
//                allocation table.
//
// Inputs       : none
// Outputs      : 0 if successful, -1 if failure

uint16_t crud_format(void) {

	//Declare variables 
	CrudResponse ret;
	CrudRequest send;
	int size = sizeof(crud_file_table[0])*CRUD_MAX_TOTAL_FILES;

	//Init the crud device
	if(init == 0)
	{
		send = crud_formati( 0, CRUD_INIT,0, 0, 0);
		ret = crud_client_operation(send, NULL);
		if( get_ret(ret) == 1)
			return -1;
		init = 1;
	}

	//Format the device 
	send = crud_formati( 0, CRUD_FORMAT,0, 0, 0);
	ret = crud_client_operation(send, NULL);
	if( get_ret(ret) == 1)
		return -1;
	//Empty the table handler
	memset(crud_file_table, 0, size);
	//Create file system
	send = crud_formati( 0, CRUD_CREATE,size, CRUD_PRIORITY_OBJECT, 0);
	ret = crud_client_operation(send, crud_file_table);
	if( get_ret(ret) == 1)
		return -1;	
	p_obj = get_oid(ret);
	// Log, return successfully
	logMessage(LOG_INFO_LEVEL, "... formatting complete.");
	return(0);
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_mount
// Description  : This function mount the current crud file system and loads
//                the file allocation table.
//
// Inputs       : none
// Outputs      : 0 if successful, -1 if failure

uint16_t crud_mount(void) {

	//Declare the variable 
	CrudResponse ret;
	CrudRequest send;
	char * buffer;
	uint32_t size =sizeof(crud_file_table[0])*CRUD_MAX_TOTAL_FILES;
	buffer = malloc(size);
	// Init the device 
	if(init == 0)
	{
		send = crud_formati( 0, CRUD_INIT,0,0,0);//size, 0, 0);
		ret = crud_client_operation(send, buffer);
		if( get_ret(ret) == 1)
			return -1;
		init = 1;
	}
	//Read the entire device
	send = crud_formati(0, CRUD_READ, size, CRUD_PRIORITY_OBJECT, 0);
	ret = crud_client_operation(send,buffer);
	if( get_ret(ret) == 1)
		return -1;
	//copy device to local drive
	memcpy(crud_file_table, buffer, size);
	free(buffer);
	// Log, return successfully
	logMessage(LOG_INFO_LEVEL, "... mount complete.");
	return(0);
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_unmount
// Description  : This function unmounts the current crud file system and
//                saves the file allocation table.
//
// Inputs       : none
// Outputs      : 0 if successful, -1 if failure

uint16_t crud_unmount(void) {

	//Declare variables
	CrudResponse ret;
	CrudRequest send;
	char * buffer;
	uint32_t size =sizeof(crud_file_table[0])*CRUD_MAX_TOTAL_FILES;
	//Store local drive to buffer
	buffer = malloc(size);
	memcpy(buffer,crud_file_table, size);
	//send buffer to the crud device
	send = crud_formati(0,CRUD_UPDATE, size, CRUD_PRIORITY_OBJECT, 0);
	ret = crud_client_operation(send, buffer);
	if( get_ret(ret) == 1)
		return -1;
	//close crud device
	send = crud_formati(0,CRUD_CLOSE, 0, 0, 0);
	ret = crud_client_operation(send, NULL);
	if( get_ret(ret) == 1)
		return -1;

	free(buffer);
	// Log, return successfully
	logMessage(LOG_INFO_LEVEL, "... unmount complete.");
	return (0);
}

// *** INSERT YOUR CODE HERE ***

// Module local methods

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crudIOUnitTest
// Description  : Perform a test of the CRUD IO implementation
//
// Inputs       : None
// Outputs      : 0 if successful or -1 if failure

int crudIOUnitTest(void) {

	// Local variables
	uint8_t ch;
	int16_t fd, i;
	int32_t cio_utest_length, cio_utest_position, count, bytes, expected;
	char *cio_utest_buffer, *tbuf;
	CRUD_UNIT_TEST_TYPE cmd;
	char lstr[1024];

	// Setup some operating buffers, zero out the mirrored file contents
	cio_utest_buffer = malloc(CRUD_MAX_OBJECT_SIZE);
	tbuf = malloc(CRUD_MAX_OBJECT_SIZE);
	memset(cio_utest_buffer, 0x0, CRUD_MAX_OBJECT_SIZE);
	cio_utest_length = 0;
	cio_utest_position = 0;

	// Format and mount the file system
	if (crud_format() || crud_mount()) {
		logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Failure on format or mount operation.");
		return(-1);
	}

	// Start by opening a file
	fd = crud_open("temp_file.txt");
	if (fd == -1) {
		logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Failure open operation.");
		return(-1);
	}

	// Now do a bunch of operations
	for (i=0; i<CRUD_IO_UNIT_TEST_ITERATIONS; i++) {

		// Pick a random command
		if (cio_utest_length == 0) {
			cmd = CIO_UNIT_TEST_WRITE;
		} else {
			cmd = getRandomValue(CIO_UNIT_TEST_READ, CIO_UNIT_TEST_SEEK);
		}

		// Execute the command
		switch (cmd) {

		case CIO_UNIT_TEST_READ: // read a random set of data
			count = getRandomValue(0, cio_utest_length);
			logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : read %d at position %d", bytes, cio_utest_position);
			bytes = crud_read(fd, tbuf, count);
			if (bytes == -1) {
				logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Read failure.");
				return(-1);
			}

			// Compare to what we expected
			if (cio_utest_position+count > cio_utest_length) {
				expected = cio_utest_length-cio_utest_position;
			} else {
				expected = count;
			}
			if (bytes != expected) {
				logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : short/long read of [%d!=%d]", bytes, expected);
				return(-1);
			}
			if ( (bytes > 0) && (memcmp(&cio_utest_buffer[cio_utest_position], tbuf, bytes)) ) {

				bufToString((unsigned char *)tbuf, bytes, (unsigned char *)lstr, 1024 );
				logMessage(LOG_INFO_LEVEL, "CIO_UTEST R: %s", lstr);
				bufToString((unsigned char *)&cio_utest_buffer[cio_utest_position], bytes, (unsigned char *)lstr, 1024 );
				logMessage(LOG_INFO_LEVEL, "CIO_UTEST U: %s", lstr);

				logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : read data mismatch (%d)", bytes);
				return(-1);
			}
			logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : read %d match", bytes);


			// update the crud_file_table[fd].positionition pointer
			cio_utest_position += bytes;
			break;

		case CIO_UNIT_TEST_APPEND: // Append data onto the end of the file
			// Create random block, check to make sure that the write is not too large
			ch = getRandomValue(0, 0xff);
			count =  getRandomValue(1, CIO_UNIT_TEST_MAX_WRITE_SIZE);
			if (cio_utest_length+count >= CRUD_MAX_OBJECT_SIZE) {

				// Log, seek to end of file, create random value
				logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : append of %d bytes [%x]", count, ch);
				logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : seek to position %d", cio_utest_length);
				if (crud_seek(fd, cio_utest_length)) {
					logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : seek failed [%d].", cio_utest_length);
					return(-1);
				}
				cio_utest_position = cio_utest_length;
				memset(&cio_utest_buffer[cio_utest_position], ch, count);

				// Now write
				bytes = crud_write(fd, &cio_utest_buffer[cio_utest_position], count);
				if (bytes != count) {
					logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : append failed [%d].", count);
					return(-1);
				}
				cio_utest_length = cio_utest_position += bytes;
			}
			break;

		case CIO_UNIT_TEST_WRITE: // Write random block to the file
			ch = getRandomValue(0, 0xff);
			count =  getRandomValue(1, CIO_UNIT_TEST_MAX_WRITE_SIZE);
			// Check to make sure that the write is not too large
			if (cio_utest_length+count < CRUD_MAX_OBJECT_SIZE) {
				// Log the write, perform it
				logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : write of %d bytes [%x]", count, ch);
				memset(&cio_utest_buffer[cio_utest_position], ch, count);
				bytes = crud_write(fd, &cio_utest_buffer[cio_utest_position], count);
				if (bytes!=count) {
					logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : write failed [%d] .", count, bytes);
					return(-1);
				}
				cio_utest_position += bytes;
				if (cio_utest_position > cio_utest_length) {
					cio_utest_length = cio_utest_position;
				}
			}
			break;

		case CIO_UNIT_TEST_SEEK:
			count = getRandomValue(0, cio_utest_length);
			logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : seek to position %d", count);
			if (crud_seek(fd, count)) {
				logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : seek failed [%d].", count);
				return(-1);
			}
			cio_utest_position = count;
			break;

		default: // This should never happen
			CMPSC_ASSERT0(0, "CRUD_IO_UNIT_TEST : illegal test command.");
			break;

		}

#if DEEP_DEBUG
		// VALIDATION STEP: ENSURE OUR LOCAL IS LIKE OBJECT STORE
		CrudRequest request;
		CrudResponse response;
		CrudOID oid;
		CRUD_REQUEST_TYPES req;
		uint32_t length;
		uint8_t res, flags;

		// Make a fake request to get file handle, then check it
		request = construct_crud_request(crud_file_table[0].object_id, CRUD_READ, CRUD_MAX_OBJECT_SIZE, CRUD_NULL_FLAG, 0);
		response = crud_client_operation(request, tbuf);
		if ((deconstruct_crud_request(response, &oid, &req, &length, &flags, &res) != 0) || (res != 0))  {
			logMessage(LOG_ERROR_LEVEL, "Read failure, bad CRUD response [%x]", response);
			return(-1);
		}
		if ( (cio_utest_length != length) || (memcmp(cio_utest_buffer, tbuf, length)) ) {
			logMessage(LOG_ERROR_LEVEL, "Buffer/Object cross validation failed [%x]", response);
			bufToString((unsigned char *)tbuf, length, (unsigned char *)lstr, 1024 );
			logMessage(LOG_INFO_LEVEL, "CIO_UTEST VR: %s", lstr);
			bufToString((unsigned char *)cio_utest_buffer, length, (unsigned char *)lstr, 1024 );
			logMessage(LOG_INFO_LEVEL, "CIO_UTEST VU: %s", lstr);
			return(-1);
		}

		// Print out the buffer
		bufToString((unsigned char *)cio_utest_buffer, cio_utest_length, (unsigned char *)lstr, 1024 );
		logMessage(LOG_INFO_LEVEL, "CIO_UTEST: %s", lstr);
#endif

	}

	// Close the files and cleanup buffers, assert on failure
	if (crud_close(fd)) {
		logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Failure read comparison block.", fd);
		return(-1);
	}
	free(cio_utest_buffer);
	free(tbuf);

	// Format and mount the file system
	if (crud_unmount()) {
		logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Failure on unmount operation.");
		return(-1);
	}

	// Return successfully
	return(0);
}




////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_open
// Description  : This function opens the file and returns a file handle
//
// Inputs       : path - the path "in the storage array"
// Outputs      : file handle if successful, -1 if failure

int16_t crud_open(char *path) {
	// Set values for the file descrptor
	int i= 0;
	while(i < CRUD_MAX_TOTAL_FILES)
	{
		if(strcmp(path,crud_file_table[i].filename)==0)
		{
			break;
		}
		i++;
	}
	if(i  >= CRUD_MAX_TOTAL_FILES)
	{
		for(  i = 0; strcmp(crud_file_table[i].filename,"") !=0; i++){}
		crud_file_table[i].length = 0;
		strcpy(crud_file_table[i].filename , path);
		crud_file_table[i].object_id = 0;
	}
	crud_file_table[i].position = 0;


	// Call a request to init the fd
	if(init == 0)
	{
		CrudRequest cr;
		cr = crud_formati( 0, CRUD_INIT,crud_file_table[i].length, 0, 0);
		CrudResponse cret = crud_client_operation( cr, NULL);
		init = 1;
		
		if((cret & 1) == 1)
		{
			return -1;
		}
	}
	crud_file_table[i].open = 1;
	// Return fd
	return i;

}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_close
// Description  : This function closes the file
//
// Inputs       : fd - the file handle of the object to close
// Outputs      : 0 if successful, -1 if failure

int16_t crud_close(int16_t fd) {
	crud_file_table[fd].open = 0;
	return 0;
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_read
// Description  : Reads up to "count" bytes from the file handle "fd" into the
//                buffer  "buf".
//
// Inputs       : fd - the file descriptor for the read
//                buf - the buffer to place the bytes into
//                count - the number of bytes to read
// Outputs      : the number of bytes read or -1 if failures

int32_t crud_read(int16_t fd, void *buf, int32_t count) {
	// Check the fd is right
	if(crud_file_table[fd].open == 0)
		return -1;
	//initialize variables 
	CrudResponse ret;
	CrudRequest send;
	char* buf2;
	// Check the count and buf is right
	if(buf == NULL || count < 0)
	{
		return -1;
	}
	// crud_file_table[fd].position and len are fixed when crud_file_table[fd].position >= len
	if(crud_file_table[fd].position >= crud_file_table[fd].length)
	{
		crud_file_table[fd].position = crud_file_table[fd].length;
		return 0;
	}
	// calculate the count when read calls for more then available spots
	if((crud_file_table[fd].position + count) > crud_file_table[fd].length){
		count =   crud_file_table[fd].length - crud_file_table[fd].position;
	}
	// call request with read and make a buffer to hold them
	buf2 = malloc( sizeof(buf[0]) * CRUD_MAX_OBJECT_SIZE);
	send = crud_formati(crud_file_table[fd].object_id, CRUD_READ, crud_file_table[fd].length,0,0);
	ret = crud_client_operation(send, buf2);
	if( get_ret(ret) == 1)
		return -1;
	//copy the read info into the buffer passed
	memcpy(buf, &buf2[ crud_file_table[fd].position],count);
	crud_file_table[fd].position = count + crud_file_table[fd].position;
	//free the buffer
	free(buf2);
	return count;

}
//////////////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_write
// Description  : Writes "count" bytes to the file handle "fd" from the
//                buffer  "buf"
//
// Inputs       : fd - the file descriptor for the file to write to
//                buf - the buffer to write
//                count - the number of bytes to write
// Outputs      : the number of bytes written or -1 if failure

int32_t crud_write(int16_t fd, void *buf, int32_t count) {
	//check the fd is right
	if(crud_file_table[fd].open == 0)
		return -1;
	//initialize variable
	CrudRequest send;
	CrudResponse ret;
	int temp;
	//check the count and buf are right
	if( buf == NULL || count <= 0)
	{
		return -1;
	}
	// Call request with creates the  Object
	if(crud_file_table[fd].length == 0)
	{
		send = crud_formati(0,CRUD_CREATE,count,0,0);
		ret = crud_client_operation(send, buf);
		if( get_ret(ret) == 1)
			return -1;
		crud_file_table[fd].position = count;
		crud_file_table[fd].length  =get_len(ret) ;
		crud_file_table[fd].object_id = get_oid(ret);
		return get_len(ret);
		
	}

	else
	{
		// Make a new buffer to hold temp 
		char *buf2;
		
		if((crud_file_table[fd].position + count) > crud_file_table[fd].length )
		{

			//Make Request to read
			buf2 = malloc( sizeof(buf[0]) * (crud_file_table[fd].position + count));
			send = crud_formati(crud_file_table[fd].object_id, CRUD_READ, crud_file_table[fd].length,0,0);
			ret = crud_client_operation(send, buf2);
			if( get_ret(ret) == 1)
				return -1;
			//Make Request to Create
			send = crud_formati(0,CRUD_CREATE,count+crud_file_table[fd].position,0,0);
			ret = crud_client_operation(send, buf2);
			if( get_ret(ret) == 1)
				return -1;
			// Change the len and write the buf
			crud_file_table[fd].length = get_len(ret);
			memcpy(&buf2[crud_file_table[fd].position], buf, count);
			//get old object idea
			temp = get_oid(ret);
			// Call Request to delete
			send = crud_formati(crud_file_table[fd].object_id,CRUD_DELETE,0,0,0);
			ret = crud_client_operation(send, NULL);
			if( get_ret(ret) == 1)
				return -1;
			//Change values for the fd
			crud_file_table[fd].object_id = temp;
			crud_file_table[fd].position = crud_file_table[fd].length;
			//make request to delete
			send = crud_formati(crud_file_table[fd].object_id,CRUD_UPDATE, crud_file_table[fd].length, 0, 0);
			ret = crud_client_operation(send, buf2);
			if( get_ret(ret) == 1)
				return -1;
			//free buf2
			free(buf2);
		}

		else
		{
			//make Request to read
			buf2 = malloc( sizeof(buf[0]) * crud_file_table[fd].length);
			send = crud_formati(crud_file_table[fd].object_id, CRUD_READ, crud_file_table[fd].length,0,0);
			ret = crud_client_operation(send, buf2);
			if( get_ret(ret) == 1)
				return -1;
			//copy buf to read 
			memcpy(&buf2[crud_file_table[fd].position], buf, count);
			//make request to update
			crud_file_table[fd].position = count + crud_file_table[fd].position;
			send = crud_formati(crud_file_table[fd].object_id,CRUD_UPDATE, crud_file_table[fd].length, 0, 0);
			ret = crud_client_operation(send, buf2);
			if( get_ret(ret) == 1)
				return -1;
			free(buf2);
		}

		return count;
	}

}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_seek
// Description  : Seek to specific point in the file
//
// Inputs       : fd - the file descriptor for the file to seek
//                loc - offset from beginning of file to seek to
// Outputs      : 0 if successful or -1 if failure

int32_t crud_seek(int16_t fd, uint32_t loc) {

	// Check the fd is right 
	if(crud_file_table[fd].open == 0)
		return -1;
	// Check the loc is right
	if(loc < 0 || loc > crud_file_table[fd].length)
	{
		return -1;
	}
	//seek the fd
	crud_file_table[fd].position = loc;
	return 0;
}


































