#include "Date.h"
#include "PNode.h"
#include "Socket.h"
#include "PQuery.h"
#include <yajl/yajl_parse.h>  
#include <yajl/yajl_gen.h>

#define MAX_MULTIWRITE_NESTING 20

typedef struct
{
  PNode *parents[MAX_MULTIWRITE_NESTING];
  int length;
  Datum *lastKey;
  int writen;
} PPNode;

PNode *PPNode_current(PPNode *self) {
  return self->parents[self->length];
}

void PPNode_go(PPNode *self, PNode *child) {
  self->length++;
  self->parents[self->length] = child;
}

void PPNode_downOrCreate(PPNode *self) {
  if (!self->writen) {
    PNode *newChild = PNode_clone(PPNode_current(self));
    PNode_createMoveToKey_(newChild, self->lastKey);
    self->writen = 1;
    PPNode_go(self, newChild);
  }
}

void PPNode_down(PPNode *self, Datum *key) {
  
  PPNode_downOrCreate(self);
  Datum_copy_(self->lastKey, key);
  self->writen = 0;
}

void PPNode_up(PPNode *self) {
  self->length--;
}

void PPNode_write(PPNode *self, Datum *value) {
  PNode_atPut_(PPNode_current(self), self->lastKey, value);
  self->writen = 1;
}

PPNode *PPNode_new(void) {
  PPNode *self = calloc(1, sizeof(PPNode));
  self->length = -1;
  self->lastKey = Datum_new();
  self->writen = 1;
  return self;
}

static int reformat_null(void * ppnode)
{
    PPNode *self = (PPNode*) ppnode;
    return 1;
}

static int reformat_string(void * ppnode, const unsigned char * stringVal,
                           unsigned int stringLen)
{
    PPNode* self = (PPNode*) ppnode;
    Datum *value = Datum_new();
    Datum_setData_size_(value, stringVal, stringLen);
    
    PPNode_write(self, value);
    return 1;
}

static int reformat_boolean(void * ppnode, int boolean)
{
    PPNode* self = (PPNode*) ppnode;
    if (boolean) {
      reformat_string(ppnode, "true", 4);
    } else {
      reformat_string(ppnode, "false", 5);
    }
    
    return 1;
}

static int reformat_number(void * ppnode, const char * s, unsigned int l)
{
  return reformat_string(ppnode, s, l);
}

static int reformat_map_key(void* ppnode, const unsigned char * stringVal,
                            unsigned int stringLen)
{
    PPNode* self = (PPNode*) ppnode;
    
    Datum *value = Datum_new();
    
    Datum_setData_size_(value, stringVal, stringLen);
    
    PPNode_down(self, value);
    return 1;
}

static int reformat_start_map(void * ppnode)
{  
    PPNode* self = (PPNode*) ppnode;
    return 1;
}


static int reformat_end_map(void * ppnode)
{
    PPNode* self = (PPNode*) ppnode;
    PPNode_up(self);
    return 1;
}

static int reformat_start_array(void * ppnode)
{
    PPNode* self = (PPNode*) ppnode;
    return 1;
}

static int reformat_end_array(void * ppnode)
{
    PPNode* self = (PPNode*) ppnode;
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


int VertexServer_api_write_hash(VertexServer *self)
{
	PNode *node  = PDB_allocNode(self->pdb);
	Datum *key   = HttpRequest_queryValue_(self->httpRequest, "key");
	Datum *value = HttpRequest_queryValue_(self->httpRequest, "value");
	Datum *post  = HttpRequest_postData(self->httpRequest);
	
  yajl_handle hand;   
  yajl_status stat;
  
  yajl_parser_config cfg = { 1, 1 }; 
	
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
	
  PNode_createMoveToKey_(node, key);
	
  PPNode *nodeSet = PPNode_new();
  
	hand = yajl_alloc(&callbacks, &cfg,  NULL, (void *) nodeSet);
	
  PPNode_go(nodeSet, node);
  
	stat = yajl_parse(hand, value->data, Datum_size(value));
	
	if (stat == yajl_status_ok) {
    Log_Printf("parsed\n");
	}
  
	return 0;
}