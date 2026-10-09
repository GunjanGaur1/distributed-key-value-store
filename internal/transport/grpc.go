package transport

import (
	"context"
	"encoding/json"

	"distributed-kv-store/internal/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
)

const serviceName = "kv.Node"

type jsonCodec struct{}

func (jsonCodec) Name() string                       { return "json" }
func (jsonCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

func init() { encoding.RegisterCodec(jsonCodec{}) }

type Service interface {
	RequestVote(context.Context, *raft.VoteRequest) (*raft.VoteResponse, error)
	AppendEntries(context.Context, *raft.AppendRequest) (*raft.AppendResponse, error)
	Client(context.Context, *raft.ClientRequest) (*raft.ClientResponse, error)
	Health() raft.HealthResponse
}

func Register(server *grpc.Server, impl Service) {
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: serviceName,
		HandlerType: (*Service)(nil),
		Methods: []grpc.MethodDesc{
			{MethodName: "RequestVote", Handler: voteHandler},
			{MethodName: "AppendEntries", Handler: appendHandler},
			{MethodName: "Client", Handler: clientHandler},
			{MethodName: "Health", Handler: healthHandler},
		},
	}, impl)
}

func voteHandler(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	in := new(raft.VoteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	return srv.(Service).RequestVote(ctx, in)
}
func appendHandler(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	in := new(raft.AppendRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	return srv.(Service).AppendEntries(ctx, in)
}
func clientHandler(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	in := new(raft.ClientRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	return srv.(Service).Client(ctx, in)
}
func healthHandler(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	return srv.(Service).Health(), nil
}

type Client struct{}

func NewClient() *Client { return &Client{} }

func (c *Client) conn(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.ForceCodec(jsonCodec{})))
}
func (c *Client) RequestVote(ctx context.Context, addr string, req *raft.VoteRequest) (*raft.VoteResponse, error) {
	conn, err := c.conn(ctx, addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	out := new(raft.VoteResponse)
	err = conn.Invoke(ctx, "/"+serviceName+"/RequestVote", req, out)
	return out, err
}
func (c *Client) AppendEntries(ctx context.Context, addr string, req *raft.AppendRequest) (*raft.AppendResponse, error) {
	conn, err := c.conn(ctx, addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	out := new(raft.AppendResponse)
	err = conn.Invoke(ctx, "/"+serviceName+"/AppendEntries", req, out)
	return out, err
}
func (c *Client) ClientCall(ctx context.Context, addr string, req *raft.ClientRequest) (*raft.ClientResponse, error) {
	conn, err := c.conn(ctx, addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	out := new(raft.ClientResponse)
	err = conn.Invoke(ctx, "/"+serviceName+"/Client", req, out)
	return out, err
}
func (c *Client) Health(ctx context.Context, addr string) (*raft.HealthResponse, error) {
	conn, err := c.conn(ctx, addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	out := new(raft.HealthResponse)
	err = conn.Invoke(ctx, "/"+serviceName+"/Health", struct{}{}, out)
	return out, err
}
