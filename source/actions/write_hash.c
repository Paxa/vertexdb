#include "Date.h"
#include "Socket.h"
#include "PQuery.h"

#include <yajl/yajl_parse.h>  
#include <yajl/yajl_gen.h>


static int reformat_null(void * ctx)
{
    yajl_gen g = (yajl_gen) ctx;
    yajl_gen_null(g);
    return 1;
}

static int reformat_boolean(void * ctx, int boolean)
{
    yajl_gen g = (yajl_gen) ctx;
    yajl_gen_bool(g, boolean);
    return 1;
}

static int reformat_number(void * ctx, const char * s, unsigned int l)
{
    yajl_gen g = (yajl_gen) ctx;
    Log_Printf__("number %s %i \n", s, l);
    yajl_gen_number(g, s, l);
    return 1;
}

static int reformat_string(void * ctx, const unsigned char * stringVal,
                           unsigned int stringLen)
{
    yajl_gen g = (yajl_gen) ctx;
    Log_Printf__("srting %s %i \n", stringVal, stringLen);
    yajl_gen_string(g, stringVal, stringLen);
    return 1;
}

static int reformat_map_key(void * ctx, const unsigned char * stringVal,
                            unsigned int stringLen)
{
    yajl_gen g = (yajl_gen) ctx;
    Log_Printf__("key %s %i \n", stringVal, stringLen);
    yajl_gen_string(g, stringVal, stringLen);
    return 1;
}

static int reformat_start_map(void * ctx)
{
    yajl_gen g = (yajl_gen) ctx;
    Log_Printf("start_map \n");
    yajl_gen_map_open(g);
    return 1;
}


static int reformat_end_map(void * ctx)
{
    yajl_gen g = (yajl_gen) ctx;
    Log_Printf("end_map \n");
    yajl_gen_map_close(g);
    return 1;
}

static int reformat_start_array(void * ctx)
{
    yajl_gen g = (yajl_gen) ctx;
    yajl_gen_array_open(g);
    return 1;
}

static int reformat_end_array(void * ctx)
{
    yajl_gen g = (yajl_gen) ctx;
    yajl_gen_array_close(g);
    return 1;
}

static yajl_callbacks callbacks = {
    reformat_null,
    reformat_boolean,
    NULL,
    NULL,
    reformat_number,
    reformat_string,
    reformat_start_map,
    reformat_map_key,
    reformat_end_map,
    reformat_start_array,
    reformat_end_array
};

/*
  Writes many of values by 1 request
  Data given from recieved post data in json format
  
  Call as:
  /users?action=write_hash&key=12
  POST {"_first_name": "Peter", "_last_name": "Bishop"}
*/
int VertexServer_api_write_hash(VertexServer *self)
{
	PNode *node = PDB_allocNode(self->pdb);
	Datum *key   = HttpRequest_queryValue_(self->httpRequest, "key");
	Datum *value = HttpRequest_queryValue_(self->httpRequest, "value");
	Datum *post  = HttpRequest_postData(self->httpRequest);
	
  yajl_handle hand;  
  static unsigned char fileData[65536];  
  /* generator config */  
  yajl_gen_config conf = { 1, "  " };  
  yajl_gen g;  
  yajl_status stat;  
  size_t rd;  
  /* allow comments */  
  yajl_parser_config cfg = { 1, 1 }; 
	
	g = yajl_gen_alloc(&conf, NULL);
	hand = yajl_alloc(&callbacks, &cfg,  NULL, (void *) g);  
	
	if(Datum_size(post) != 0)
	{
		value = post;
	}

	if (PNode_moveToPathIfExists_(node, HttpRequest_uriPath(self->httpRequest)) != 0) 
	{
		VertexServer_setErrorCString_(self, "{\"error\": [4, \"write path does not exist\"]}");
		VertexServer_appendError_(self, HttpRequest_uriPath(self->httpRequest));
		return -1;
	}
	
	stat = yajl_parse(hand, value->data, Datum_size(value));
	
	if (stat == yajl_status_ok) {
    Log_Printf("parsed\n");
    
    const unsigned char * buf;  
    unsigned int len;  
    yajl_gen_get_buf(g, &buf, &len);  
    fwrite(buf, 1, len, stdout);  
    yajl_gen_clear(g);
	}
	
  Log_Printf_("writing %s\n", value->data);
  
  /*
	if(Datum_equalsCString_(mode, "append"))
	{
		PNode_atCat_(node, key, value);
	}
	else if(Datum_equalsCString_(mode, "meta"))
	{
		PNode_metaAt_put_(node, key, value);
	}
	else
	{
		PNode_atPut_(node, key, value);
	}
	*/
	return 0;
}