#include "HttpServer.h"
#include "Log.h"
#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include <sys/types.h>
#include <sys/time.h>
//#include <sys/queue.h>
#include <stdlib.h>
#include <math.h>
//#include <limits.h>
//#include <signal.h>
#include <unistd.h>
#include "Pool.h"
#include "PNode.h"

void *CHash_atString_(CHash *self, const char *s)
{
	DATUM_STACKALLOCATED(k, s);
	return CHash_at_(self, k);
}

#define HTTP_SERVERERROR 500
#define HTTP_SERVERERROR_MESSAGE "Internal Server Error"
#define HTTP_OK_MESSAGE	"OK"

ThreadBox *ThreadBox_new(void) {
  ThreadBox *self = calloc(1, sizeof(ThreadBox));
	self->httpRequest = HttpRequest_new();
	self->httpResponse = HttpResponse_new();
	self->result = HttpResponse_content(self->httpResponse);
}

HttpServer *HttpServer_new(void)
{
  
	HttpServer *self = calloc(1, sizeof(HttpServer));
	
  long num;
  for (num = 0; num < THREADS_NUM; num ++) {
    globalHTTPRunners[num] = ThreadBox_new();
    globalHTTPRunners[num]->number = num;
    globalHTTPRunners[num]->httpServer = self;
  }
  globalCurrentThread = globalHTTPRunners[0];
  
	self->httpRequest = HttpRequest_new();
	self->httpResponse = HttpResponse_new();
	self->host = Datum_new();
	return self;
}

void HttpServer_free(HttpServer *self)
{
	HttpRequest_free(self->httpRequest);
	HttpResponse_free(self->httpResponse);
	Datum_free(self->host);
	if (self->httpd) evhttp_free(self->httpd);
	free(self);
}

HttpRequest *HttpServer_request(HttpServer *self)
{
	return self->httpRequest;
}

HttpResponse *HttpServer_response(HttpServer *self)
{
	return self->httpResponse;
}

void HttpServer_setHost_(HttpServer *self, char *host)
{
	Datum_setCString_(self->host, host);
}

Datum *HttpServer_host(HttpServer *self)
{
	return self->host;
}

void HttpServer_setPort_(HttpServer *self, int port)
{
	self->port = port;
}

int HttpServer_port(HttpServer *self)
{
	return self->port;
}

void HttpServer_setDelegate_(HttpServer *self, void *d)
{
	self->delegate = d;
}

void *HttpServer_delegate(HttpServer *self)
{
	return self->delegate;
}

void HttpServer_setRequestCallback_(HttpServer *self, HttpServerRequestCallback *f)
{
	self->requestCallback = f;
}

void HttpServer_setIdleCallback_(HttpServer *self, HttpServerIdleCallback *f)
{
	self->idleCallback = f;
}

void *HttpServer_threadRunner(void *number)
{
  long tid;
  tid = (long)number;
  ThreadBox *self = globalHTTPRunners[tid];
	HttpServer *serv = self->httpServer;
	
  const char *uri;
	struct evbuffer *buf = evbuffer_new();
	
  while(!serv->shutdown) {
    if (self->gotRequest) {
      uri = evhttp_request_uri(self->request);
    	Log_Printf_("url: %s \n", uri);
    	
      evbuffer_free(buf);
      
    	HttpRequest_readPostData(self->httpRequest);
    	
    	HttpResponse_clear(self->httpResponse);

    	if (strcmp(uri, "/favicon.ico") == 0)
    	{
    		evhttp_send_reply(self->request, HTTP_OK, HTTP_OK_MESSAGE, buf);
    	}
    	else
    	{
    		HttpRequest_parseUri_(self->httpRequest, uri);
    		serv->requestCallback(serv->delegate, self);
        Log_Printf_("result: %s\n", Datum_data(HttpResponse_content(self->httpResponse)));

    		if (Datum_size(self->httpResponse->callback)) {
    			evbuffer_add_printf(buf, "%s(%s)", Datum_data(self->httpResponse->callback), Datum_data(HttpResponse_content(self->httpResponse)));
    		} else {
    			evbuffer_add_printf(buf, "%s", Datum_data(HttpResponse_content(self->httpResponse)));
    		}

    		if (HttpResponse_statusCode(self->httpResponse) == 200)
    		{
    			evhttp_send_reply(self->request, HTTP_OK, HTTP_OK_MESSAGE, buf);
    		}
    		else
    		{
    			evhttp_send_reply(self->request, HTTP_SERVERERROR, HTTP_SERVERERROR_MESSAGE, buf);		
    		}
    	}
      
    	evhttp_send_reply_end(self->request);
    	evbuffer_free(buf);
    	Datum_poolFreeRefs();
    	PNode_poolFreeRefs();
      self->gotRequest = 0;
    }
  }
}

void HttpServer_handleRequest(struct evhttp_request *req, void *arg)  
{
  Log_Printf_("got request, send to %i\n", (int)globalCurrentThread->number);
  globalCurrentThread->request = req;
	globalCurrentThread->httpRequest->request = req;
	globalCurrentThread->httpResponse->request = req;
	
  globalCurrentThread->gotRequest = 1;
  if (globalCurrentThread->number == 11) {
    globalCurrentThread = globalHTTPRunners[0];
  } else {
    globalCurrentThread = globalHTTPRunners[globalCurrentThread->number + 1];
  }
}

void HttpServer_run(HttpServer *self)
{
	if(self->shutdown) return;
	self->shutdown = 0;
  int i = 0;
	event_init();

	self->httpd = evhttp_start(Datum_data(self->host), self->port);
	
  long num;
  for (num = 0; num < THREADS_NUM; num ++) {
    pthread_create(&globalHTTPRunners[num]->thread, NULL, HttpServer_threadRunner, (void *)num);
  }
	
	if (!self->httpd)
	{
		Log_Printf__("evhttp_start failed - is another copy running on port %i or wrong -host ip %s ?\n", self->port, Datum_data(self->host));
		exit(-1);
	}
	
	evhttp_set_timeout(self->httpd, 180);
	evhttp_set_gencb(self->httpd, HttpServer_handleRequest, self);  
	
	while (!self->shutdown)
	{
		if(self->idleCallback)
		{
			self->gotRequest = 0;
			event_loop(EVLOOP_NONBLOCK);
			
			if (!self->gotRequest && self->idleCallback)
			{
				self->idleCallback(self->delegate);
			}
		}
		else 
		{
			event_loop(EVLOOP_ONCE);
		}
	}
}

void HttpServer_shutdown(HttpServer *self)
{
  int num;
  
	self->shutdown = 1;
	event_loopbreak();
}

