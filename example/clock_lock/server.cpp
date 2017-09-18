// Copyright (c) 2014 Baidu, Inc.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// A server to receive EchoRequest and send back EchoResponse.

#include <gflags/gflags.h>
#include <butil/logging.h>
#include <butil/strings/string_split.h>
#include <butil/strings/string_number_conversions.h>
#include "butil/strings/safe_sprintf.h"
#include <butil/time.h>
#include <brpc/server.h>
#include <vector>
#include <string>
#include <brpc/channel.h>
#include "echo.pb.h"
#include <stdio.h>
#include <limits>

DEFINE_bool(echo_attachment, true, "Echo attachment as well");
DEFINE_int32(port, 8000, "TCP Port of this server");
DEFINE_string(servers, "0.0.0.0:8000", "TCP endpoint of cluster servers");
DEFINE_int32(server_id, 0, "Server id");
DEFINE_int32(idle_timeout_s, -1, "Connection will be closed if there is no "
             "read/write operations during the last `idle_timeout_s'");
DEFINE_int32(logoff_ms, 2000, "Maximum duration of server's LOGOFF state "
             "(waiting for client to close connection before server stops)");
DEFINE_int32(thread_num, 1, "Number of threads to send requests");
DEFINE_bool(use_bthread, false, "Use bthread to send requests");
DEFINE_int32(attachment_size, 0, "Carry so many byte attachment along with requests");
DEFINE_int32(request_size, 16, "Bytes of each request");
DEFINE_string(protocol, "baidu_std", "Protocol type. Defined in src/brpc/options.proto");
DEFINE_string(connection_type, "", "Connection type. Available values: single, pooled, short");
DEFINE_string(server, "0.0.0.0:8002", "IP Address of server");
DEFINE_string(load_balancer, "", "The algorithm for load balancing");
DEFINE_int32(timeout_ms, 100, "RPC timeout in milliseconds");
DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)");
DEFINE_bool(dont_fail, false, "Print fatal when some call failed");
DEFINE_int32(dummy_port, 0, "Launch dummy server at this port");
DEFINE_string(http_content_type, "application/json", "Content type of http request");
DEFINE_int32(interval_ms, 1000, "Milliseconds between consecutive requests");

// Your implementation of example::EchoService
// Notice that implementing brpc::Describable grants the ability to put
// additional information in /status.
namespace example {
class EchoServiceImpl : public EchoService {
public:
    EchoServiceImpl() {};
    virtual ~EchoServiceImpl() {};
    virtual void Echo(google::protobuf::RpcController* cntl_base,
                      const EchoRequest* request,
                      EchoResponse* response,
                      google::protobuf::Closure* done) {
        // This object helps you to call done->Run() in RAII style. If you need
        // to process the request asynchronously, pass done_guard.release().
        brpc::ClosureGuard done_guard(done);

        brpc::Controller* cntl =
            static_cast<brpc::Controller*>(cntl_base);

        // The purpose of following logs is to help you to understand
        // how clients interact with servers more intuitively. You should 
        // remove these logs in performance-sensitive servers.
        // You should also noticed that these logs are different from what
        // we wrote in other projects: they use << instead of printf-style
        // functions. But don't worry, these logs are fully compatible with
        // comlog. You can mix them with comlog or ullog functions freely.
        // The noflush prevents the log from being flushed immediately.
        LOG(INFO) << "Received request[log_id=" << cntl->log_id() 
                  << "] from " << cntl->remote_side() 
                  << " to " << cntl->local_side() << noflush;
        LOG(INFO) << ": " << request->message() << noflush;
        if (!cntl->request_attachment().empty()) {
            LOG(INFO) << " (attached=" << cntl->request_attachment() << ")" << noflush;
        }
        LOG(INFO);

        // Fill response.
        response->set_message(request->message());

        // You can compress the response by setting Controller, but be aware
        // that compression may be costly, evaluate before turning on.
        // cntl->set_response_compress_type(brpc::COMPRESS_TYPE_GZIP);

        if (FLAGS_echo_attachment) {
            // Set attachment which is wired to network directly instead of
            // being serialized into protobuf messages.
            cntl->response_attachment().append(cntl->request_attachment());
        }
    }
};
}  // namespace example
std::vector<std::string> g_servers;
void init_servers(std::string server_list){
    butil::SplitString(server_list,',',&g_servers);
    for (auto i = g_servers.begin(); i != g_servers.end(); ++i){
        LOG(INFO) << "g_servers: " << *i << noflush;
    }
}
int g_port;
std::string g_server;
int g_server_id;
void init_server_port(int server_id){
    std::vector<std::string> result;
    butil::SplitString(g_servers[server_id],':',&result);
    butil::StringToInt(result[1],&g_port);
    g_server = g_servers[server_id];
    g_server_id = server_id;
}

void send_msg(std::string endpoint,std::string msg,example::EchoRequest *request,example::EchoResponse *response) {
    // A Channel represents a communication line to a Server. Notice that
    // Channel is thread-safe and can be shared by all threads in your program.
    brpc::Channel channel;

    // Initialize the channel, NULL means using default options.
    brpc::ChannelOptions options;
    options.protocol = FLAGS_protocol;
    options.connection_type = FLAGS_connection_type;
    options.timeout_ms = FLAGS_timeout_ms/*milliseconds*/;
    options.max_retry = FLAGS_max_retry;
    if (channel.Init(endpoint.c_str(), FLAGS_load_balancer.c_str(), &options) != 0) {
        LOG(ERROR) << "Fail to initialize channel";
        return ;
    }
    // Normally, you should not call a Channel directly, but instead construct
    // a stub Service wrapping it. stub can be shared by all threads as well.
    example::EchoService_Stub stub(&channel);

    int log_id = 0;
    if (!brpc::IsAskedToQuit()) {
        brpc::Controller cntl;

        request->set_message(msg);
        cntl.set_log_id(log_id++);  // set by user
        // Because `done'(last parameter) is NULL, this function waits until
        // the response comes back or error occurs(including timedout).
        stub.Echo(&cntl, request, response, NULL);
    }
}
static void* sender(void* ) {
    char msg[20];
    sprintf(msg,"%s%d%c","hello",g_server_id,'\0');
    while (!brpc::IsAskedToQuit()) {
	 for (int i = 0;i<static_cast<int>(g_servers.size());i++){
	    example::EchoRequest request;
	    example::EchoResponse response;
            if (i==g_server_id){
                continue;
            }
            send_msg(g_servers[i],msg,&request,&response);
            usleep(FLAGS_interval_ms * 1000L);
        }
    }
    return NULL;
}
int main(int argc, char* argv[]) {
    // Parse gflags. We recommend you to use gflags as well.
    google::ParseCommandLineFlags(&argc, &argv, true);
    init_servers(FLAGS_servers);
    init_server_port(FLAGS_server_id);
    
    // Generally you only need one Server.
    brpc::Server server;

    // Instance of your service.
    example::EchoServiceImpl echo_service_impl;

    // Add the service into server. Notice the second parameter, because the
    // service is put on stack, we don't want server to delete it, otherwise
    // use brpc::SERVER_OWNS_SERVICE.
    if (server.AddService(&echo_service_impl, 
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }

    // Start the server.
    brpc::ServerOptions options;
    options.idle_timeout_sec = FLAGS_idle_timeout_s;
    if (server.Start(g_port, &options) != 0) {
        LOG(ERROR) << "Fail to start EchoServer";
        return -1;
    }

    std::vector<bthread_t> tids;
    tids.resize(FLAGS_thread_num);
    if (!FLAGS_use_bthread) {
        for (int i = 0; i < FLAGS_thread_num; ++i) {
            if (pthread_create(&tids[i], NULL, sender,NULL) != 0) {
                LOG(ERROR) << "Fail to create pthread";
                return -1;
            }
        }
    } else {
        for (int i = 0; i < FLAGS_thread_num; ++i) {
            if (bthread_start_background(
                    &tids[i], NULL, sender,NULL) != 0) {
                LOG(ERROR) << "Fail to create bthread";
                return -1;
            }
        }
    }
    // Wait until Ctrl-C is pressed, then Stop() and Join() the server.
    server.RunUntilAskedToQuit();
    return 0;
}
