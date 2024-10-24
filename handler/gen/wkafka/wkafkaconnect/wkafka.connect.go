// Code generated by protoc-gen-connect-go. DO NOT EDIT.
//
// Source: wkafka/wkafka.proto

package wkafkaconnect

import (
	connect "connectrpc.com/connect"
	context "context"
	errors "errors"
	wkafka "github.com/worldline-go/wkafka/handler/gen/wkafka"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	http "net/http"
	strings "strings"
)

// This is a compile-time assertion to ensure that this generated file and the connect package are
// compatible. If you get a compiler error that this constant is not defined, this code was
// generated with a version of connect newer than the one compiled into your binary. You can fix the
// problem by either regenerating this code with an older version of connect or updating the connect
// version compiled into your binary.
const _ = connect.IsAtLeastVersion1_13_0

const (
	// WkafkaServiceName is the fully-qualified name of the WkafkaService service.
	WkafkaServiceName = "wkafka.WkafkaService"
)

// These constants are the fully-qualified names of the RPCs defined in this package. They're
// exposed at runtime as Spec.Procedure and as the final two segments of the HTTP route.
//
// Note that these are different from the fully-qualified method names used by
// google.golang.org/protobuf/reflect/protoreflect. To convert from these constants to
// reflection-formatted method names, remove the leading slash and convert the remaining slash to a
// period.
const (
	// WkafkaServiceSkipProcedure is the fully-qualified name of the WkafkaService's Skip RPC.
	WkafkaServiceSkipProcedure = "/wkafka.WkafkaService/Skip"
	// WkafkaServiceInfoProcedure is the fully-qualified name of the WkafkaService's Info RPC.
	WkafkaServiceInfoProcedure = "/wkafka.WkafkaService/Info"
)

// These variables are the protoreflect.Descriptor objects for the RPCs defined in this package.
var (
	wkafkaServiceServiceDescriptor    = wkafka.File_wkafka_wkafka_proto.Services().ByName("WkafkaService")
	wkafkaServiceSkipMethodDescriptor = wkafkaServiceServiceDescriptor.Methods().ByName("Skip")
	wkafkaServiceInfoMethodDescriptor = wkafkaServiceServiceDescriptor.Methods().ByName("Info")
)

// WkafkaServiceClient is a client for the wkafka.WkafkaService service.
type WkafkaServiceClient interface {
	Skip(context.Context, *connect.Request[wkafka.CreateSkipRequest]) (*connect.Response[wkafka.Response], error)
	Info(context.Context, *connect.Request[emptypb.Empty]) (*connect.Response[wkafka.InfoResponse], error)
}

// NewWkafkaServiceClient constructs a client for the wkafka.WkafkaService service. By default, it
// uses the Connect protocol with the binary Protobuf Codec, asks for gzipped responses, and sends
// uncompressed requests. To use the gRPC or gRPC-Web protocols, supply the connect.WithGRPC() or
// connect.WithGRPCWeb() options.
//
// The URL supplied here should be the base URL for the Connect or gRPC server (for example,
// http://api.acme.com or https://acme.com/grpc).
func NewWkafkaServiceClient(httpClient connect.HTTPClient, baseURL string, opts ...connect.ClientOption) WkafkaServiceClient {
	baseURL = strings.TrimRight(baseURL, "/")
	return &wkafkaServiceClient{
		skip: connect.NewClient[wkafka.CreateSkipRequest, wkafka.Response](
			httpClient,
			baseURL+WkafkaServiceSkipProcedure,
			connect.WithSchema(wkafkaServiceSkipMethodDescriptor),
			connect.WithClientOptions(opts...),
		),
		info: connect.NewClient[emptypb.Empty, wkafka.InfoResponse](
			httpClient,
			baseURL+WkafkaServiceInfoProcedure,
			connect.WithSchema(wkafkaServiceInfoMethodDescriptor),
			connect.WithClientOptions(opts...),
		),
	}
}

// wkafkaServiceClient implements WkafkaServiceClient.
type wkafkaServiceClient struct {
	skip *connect.Client[wkafka.CreateSkipRequest, wkafka.Response]
	info *connect.Client[emptypb.Empty, wkafka.InfoResponse]
}

// Skip calls wkafka.WkafkaService.Skip.
func (c *wkafkaServiceClient) Skip(ctx context.Context, req *connect.Request[wkafka.CreateSkipRequest]) (*connect.Response[wkafka.Response], error) {
	return c.skip.CallUnary(ctx, req)
}

// Info calls wkafka.WkafkaService.Info.
func (c *wkafkaServiceClient) Info(ctx context.Context, req *connect.Request[emptypb.Empty]) (*connect.Response[wkafka.InfoResponse], error) {
	return c.info.CallUnary(ctx, req)
}

// WkafkaServiceHandler is an implementation of the wkafka.WkafkaService service.
type WkafkaServiceHandler interface {
	Skip(context.Context, *connect.Request[wkafka.CreateSkipRequest]) (*connect.Response[wkafka.Response], error)
	Info(context.Context, *connect.Request[emptypb.Empty]) (*connect.Response[wkafka.InfoResponse], error)
}

// NewWkafkaServiceHandler builds an HTTP handler from the service implementation. It returns the
// path on which to mount the handler and the handler itself.
//
// By default, handlers support the Connect, gRPC, and gRPC-Web protocols with the binary Protobuf
// and JSON codecs. They also support gzip compression.
func NewWkafkaServiceHandler(svc WkafkaServiceHandler, opts ...connect.HandlerOption) (string, http.Handler) {
	wkafkaServiceSkipHandler := connect.NewUnaryHandler(
		WkafkaServiceSkipProcedure,
		svc.Skip,
		connect.WithSchema(wkafkaServiceSkipMethodDescriptor),
		connect.WithHandlerOptions(opts...),
	)
	wkafkaServiceInfoHandler := connect.NewUnaryHandler(
		WkafkaServiceInfoProcedure,
		svc.Info,
		connect.WithSchema(wkafkaServiceInfoMethodDescriptor),
		connect.WithHandlerOptions(opts...),
	)
	return "/wkafka.WkafkaService/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case WkafkaServiceSkipProcedure:
			wkafkaServiceSkipHandler.ServeHTTP(w, r)
		case WkafkaServiceInfoProcedure:
			wkafkaServiceInfoHandler.ServeHTTP(w, r)
		default:
			http.NotFound(w, r)
		}
	})
}

// UnimplementedWkafkaServiceHandler returns CodeUnimplemented from all methods.
type UnimplementedWkafkaServiceHandler struct{}

func (UnimplementedWkafkaServiceHandler) Skip(context.Context, *connect.Request[wkafka.CreateSkipRequest]) (*connect.Response[wkafka.Response], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("wkafka.WkafkaService.Skip is not implemented"))
}

func (UnimplementedWkafkaServiceHandler) Info(context.Context, *connect.Request[emptypb.Empty]) (*connect.Response[wkafka.InfoResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("wkafka.WkafkaService.Info is not implemented"))
}
