// Revised benchmark: a success means the Worker completed and returned the expected result.
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/vmihailenco/msgpack/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	fb_order "protocol-benchmark/order"
	pb "protocol-benchmark/pb"
)

const maxBody = 16 << 20
const maxItems = 10000
const callTimeout = 10 * time.Second

// These transport-neutral values are the common logical order used by every
// protocol; struct tags keep the JSON and MessagePack field names aligned.
type OrderItem struct {
	ItemID   string  `json:"item_id" msgpack:"item_id"`
	Quantity int32   `json:"quantity" msgpack:"quantity"`
	Price    float64 `json:"price" msgpack:"price"`
}

type OrderPayload struct {
	OrderID     string      `json:"order_id" msgpack:"order_id"`
	UserID      string      `json:"user_id" msgpack:"user_id"`
	Items       []OrderItem `json:"items" msgpack:"items"`
	TotalAmount float64     `json:"total_amount" msgpack:"total_amount"`
}

// All representations expose the same logical fields to the same operation.
// FlatBuffers stays a view over the buffer; no object tree is reconstructed.
// Keeping processing behind this interface makes validation and hashing
// identical regardless of the transport's in-memory representation.
type orderView interface {
	orderID() string
	userID() string
	total() float64
	count() int
	item(int) (string, int32, float64, error)
}

// The native view is used after decoding JSON or MessagePack into Go values.
type nativeView struct{ p *OrderPayload }

func (v nativeView) orderID() string { return v.p.OrderID }
func (v nativeView) userID() string  { return v.p.UserID }
func (v nativeView) total() float64  { return v.p.TotalAmount }
func (v nativeView) count() int      { return len(v.p.Items) }
func (v nativeView) item(i int) (string, int32, float64, error) {
	x := v.p.Items[i]
	return x.ItemID, x.Quantity, x.Price, nil
}

// The protobuf view reads generated message fields directly for gRPC requests.
type protoView struct{ p *pb.OrderPayload }

func (v protoView) orderID() string { return v.p.GetOrderId() }
func (v protoView) userID() string  { return v.p.GetUserId() }
func (v protoView) total() float64  { return v.p.GetTotalAmount() }
func (v protoView) count() int      { return len(v.p.GetItems()) }
func (v protoView) item(i int) (string, int32, float64, error) {
	x := v.p.Items[i]
	if x == nil {
		return "", 0, 0, errors.New("nil item")
	}
	return x.GetItemId(), x.GetQuantity(), x.GetPrice(), nil
}

// The FlatBuffers view reads fields through generated accessors over the input
// buffer, avoiding the allocation of a second object graph.
type flatView struct{ p *fb_order.OrderPayload }

func (v flatView) orderID() string { return string(v.p.OrderId()) }
func (v flatView) userID() string  { return string(v.p.UserId()) }
func (v flatView) total() float64  { return v.p.TotalAmount() }
func (v flatView) count() int      { return v.p.ItemsLength() }
func (v flatView) item(i int) (string, int32, float64, error) {
	var x fb_order.OrderItem
	if !v.p.Items(&x, i) {
		return "", 0, 0, errors.New("missing item")
	}
	return string(x.ItemId()), x.Quantity(), x.Price(), nil
}

// Canonical FNV-1a input: length-prefixed UTF-8 strings; little-endian
// uint64 item count, IEEE-754 float64 bits, and uint32 quantity bits.
// This is a deterministic workload/result check, not a cryptographic proof.
func processOrder(ctx context.Context, v orderView) (token string, err error) {
	// Generated FlatBuffers accessors may panic on malformed offsets.
	// This boundary rejects those inputs instead of returning false success.
	defer func() {
		if recover() != nil {
			token = ""
			err = errors.New("invalid order buffer")
		}
	}()
	h := fnv.New64a()
	var b [8]byte
	// Length-prefix strings and use explicit little-endian integers so distinct
	// field sequences have one stable byte representation to hash.
	put64 := func(x uint64) { binary.LittleEndian.PutUint64(b[:], x); _, _ = h.Write(b[:]) }
	putString := func(s string) { put64(uint64(len(s))); _, _ = h.Write([]byte(s)) }
	id, user, total, n := v.orderID(), v.userID(), v.total(), v.count()
	// Reject invalid root data before hashing any items.
	if id == "" || user == "" || n < 1 || n > maxItems || !finite(total) || total < 0 {
		return "", errors.New("invalid order fields")
	}
	putString(id)
	putString(user)
	put64(uint64(n))
	put64(math.Float64bits(total))
	for i := 0; i < n; i++ {
		// Check cancellation between items so large orders stop promptly.
		if err := ctx.Err(); err != nil {
			return "", err
		}
		id, q, p, err := v.item(i)
		if err != nil {
			return "", err
		}
		if id == "" || q <= 0 || !finite(p) || p < 0 {
			return "", errors.New("invalid item fields")
		}
		putString(id)
		binary.LittleEndian.PutUint32(b[:4], uint32(q))
		_, _ = h.Write(b[:4])
		put64(math.Float64bits(p))
	}
	return fmt.Sprintf("OK:%d:%016x", n, h.Sum64()), nil
}

func finite(x float64) bool { return !math.IsNaN(x) && !math.IsInf(x, 0) }

// Generate the same deterministic order for each checkout, allowing the
// gateway and worker to independently compute and compare the same result.
func generatePayload(n int) OrderPayload {
	p := OrderPayload{OrderID: "ord-123456789", UserID: "usr-987654321", Items: make([]OrderItem, n)}
	for i := range p.Items {
		q := int32(i + 1)
		price := 19.99 * float64(i+1)
		p.Items[i] = OrderItem{ItemID: fmt.Sprintf("item-%d", i), Quantity: q, Price: price}
		p.TotalAmount += float64(q) * price
	}
	return p
}

// Map the transport-neutral order to the generated protobuf shape used by gRPC.
func toProto(p OrderPayload) *pb.OrderPayload {
	items := make([]*pb.OrderItem, len(p.Items))
	for i, x := range p.Items {
		items[i] = &pb.OrderItem{ItemId: x.ItemID, Quantity: x.Quantity, Price: x.Price}
	}
	return &pb.OrderPayload{OrderId: p.OrderID, UserId: p.UserID, Items: items, TotalAmount: p.TotalAmount}
}

// JSON and MessagePack serialize the tagged Go structs; FlatBuffers constructs
// its tables explicitly in the generated schema's required order.
func encodeHTTP(protocol string, dummyData OrderPayload) ([]byte, string, error) {
	switch protocol {
	case "json":
		b, e := json.Marshal(dummyData)
		return b, "application/json", e
	case "msgpack":
		b, e := msgpack.Marshal(dummyData)
		return b, "application/x-msgpack", e
	case "flatbuffers":

		// FlatBuffers tables are assembled from their leaves toward the root.
		builder := flatbuffers.NewBuilder(1024)

		// 1. Serialize OrderItem elements (Bottom-Up construction)
		itemOffsets := make([]flatbuffers.UOffsetT, len(dummyData.Items))
		for i, item := range dummyData.Items {
			itemIDOffset := builder.CreateString(item.ItemID)

			fb_order.OrderItemStart(builder)
			fb_order.OrderItemAddItemId(builder, itemIDOffset)
			fb_order.OrderItemAddQuantity(builder, item.Quantity)
			fb_order.OrderItemAddPrice(builder, item.Price)
			itemOffsets[i] = fb_order.OrderItemEnd(builder)
		}

		// 2. Build the Items vector. Prepending in reverse preserves the
		// original item order in the finished buffer.
		fb_order.OrderPayloadStartItemsVector(builder, len(itemOffsets))
		for i := len(itemOffsets) - 1; i >= 0; i-- {
			builder.PrependUOffsetT(itemOffsets[i])
		}
		itemsVec := builder.EndVector(len(itemOffsets))

		// 3. Serialize root OrderPayload scalar/string fields
		orderIDOffset := builder.CreateString(dummyData.OrderID)
		userIDOffset := builder.CreateString(dummyData.UserID)

		// 4. Build root OrderPayload table
		fb_order.OrderPayloadStart(builder)
		fb_order.OrderPayloadAddOrderId(builder, orderIDOffset)
		fb_order.OrderPayloadAddUserId(builder, userIDOffset)
		fb_order.OrderPayloadAddItems(builder, itemsVec)
		fb_order.OrderPayloadAddTotalAmount(builder, dummyData.TotalAmount)
		payloadOffset := fb_order.OrderPayloadEnd(builder)

		// 5. Finish buffer and record true dynamic length
		builder.Finish(payloadOffset)
		buf := builder.FinishedBytes()
		return buf, "application/octet-stream", nil

	}
	return nil, "", errors.New("unsupported HTTP protocol")
}

func decodeAndProcess(ctx context.Context, protocol string, b []byte) (token string, err error) {
	// Malformed FlatBuffers offsets can panic in generated accessors; turn that
	// into a rejected request rather than allowing it to terminate the worker.
	defer func() {
		if recover() != nil {
			token = ""
			err = errors.New("invalid payload")
		}
	}()
	switch protocol {
	case "json", "msgpack":
		// Both wire formats decode into the shared native representation.
		var p OrderPayload
		if protocol == "json" {
			err = json.Unmarshal(b, &p)
		} else {
			err = msgpack.Unmarshal(b, &p)
		}
		if err != nil {
			return "", err
		}
		return processOrder(ctx, nativeView{&p})
	case "flatbuffers":
		// The FlatBuffers path keeps the request bytes as the backing storage.
		if len(b) < 8 {
			return "", errors.New("short FlatBuffers payload")
		}
		return processOrder(ctx, flatView{fb_order.GetRootAsOrderPayload(b, 0)})
	}
	return "", errors.New("unsupported protocol")
}

type grpcOrderServer struct {
	pb.UnimplementedOrderServiceServer
}

// ProcessOrder exposes the shared validator and token computation over gRPC.
func (s *grpcOrderServer) ProcessOrder(ctx context.Context, p *pb.OrderPayload) (*pb.OrderResponse, error) {
	if p == nil {
		return nil, status.Error(codes.InvalidArgument, "missing order")
	}
	token, err := processOrder(ctx, protoView{p})
	if err != nil {
		if ctx.Err() != nil {
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		return nil, status.Error(codes.InvalidArgument, "invalid order")
	}
	// Retain the existing schema. Status carries the identical completion token
	// used as the HTTP Worker response body.
	return &pb.OrderResponse{Status: token}, nil
}

func workerHandler(protocol string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/process", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", "POST")
			http.Error(w, "POST required", 405)
			return
		}
		defer r.Body.Close()
		// Bound reads before decoding so oversized requests cannot consume
		// unbounded memory; decoding errors and invalid orders are client errors.
		b, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
		if err != nil {
			http.Error(w, "invalid or oversized body", 400)
			return
		}
		token, err := decodeAndProcess(r.Context(), protocol, b)
		if err != nil {
			http.Error(w, "invalid order", 400)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, token)
	})
	return mux
}

func serveHTTP(addr string, h http.Handler) {
	// Server timeouts bound slow headers, bodies, writes, and idle connections.
	s := &http.Server{Addr: addr, Handler: h, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 90 * time.Second}
	log.Fatal(s.ListenAndServe())
}

func runWorker(protocol string) {
	if protocol == "grpc" {
		// gRPC uses its dedicated RPC server and port; other protocols share
		// the HTTP worker endpoint.
		lis, err := net.Listen("tcp", ":50051")
		if err != nil {
			log.Fatal(err)
		}
		s := grpc.NewServer(grpc.MaxRecvMsgSize(maxBody))
		pb.RegisterOrderServiceServer(s, &grpcOrderServer{})
		log.Fatal(s.Serve(lis))
		return
	}
	serveHTTP(":8080", workerHandler(protocol))
}

// Same explicit HTTP/1.1 connection policy for JSON, MessagePack, FlatBuffers.
// gRPC retains its HTTP/2 stack; the experiment compares implementations.
func newHTTPClient() *http.Client {
	return &http.Client{Timeout: callTimeout, Transport: &http.Transport{
		Proxy: nil, DialContext: (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns: 512, MaxIdleConnsPerHost: 512, IdleConnTimeout: 90 * time.Second,
		ResponseHeaderTimeout: callTimeout, ForceAttemptHTTP2: false,
	}, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
}

func gatewayHandler(protocol, host string, n int, client *http.Client, rpc pb.OrderServiceClient) http.Handler {
	// Fixed dataset: compute expected result once, outside the measured requests.
	// Payload construction and encoding still happen on every request.
	expected, err := processOrder(context.Background(), nativeView{ptrPayload(generatePayload(n))})
	if err != nil {
		panic(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/checkout", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			http.Error(w, "GET required", 405)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), callTimeout)
		defer cancel()
		// Rebuild the deterministic input for every timed benchmark request.
		p := generatePayload(n)
		var token string
		var err error
		if protocol == "grpc" {
			// gRPC carries the protobuf representation and returns the token in
			// the response's existing Status field.
			var result *pb.OrderResponse
			result, err = rpc.ProcessOrder(ctx, toProto(p))
			if err == nil {
				if result == nil {
					err = errors.New("missing worker result")
				} else {
					token = result.GetStatus()
				}
			}
		} else {
			// HTTP protocols differ only in body encoding; they use the same
			// worker endpoint and token response contract.
			var b []byte
			var contentType string
			b, contentType, err = encodeHTTP(protocol, p)
			if err == nil {
				var req *http.Request
				req, err = http.NewRequestWithContext(ctx, http.MethodPost, "http://"+net.JoinHostPort(host, "8080")+"/process", bytes.NewReader(b))
				if err == nil {
					req.Header.Set("Content-Type", contentType)
					var resp *http.Response
					resp, err = client.Do(req)
					if err == nil {
						// Fully read the bounded completion response before closing, enabling reuse.
						var body []byte
						body, err = io.ReadAll(io.LimitReader(resp.Body, 1025))
						closeErr := resp.Body.Close()
						if err == nil {
							err = closeErr
						}
						if err == nil && (resp.StatusCode != http.StatusOK || len(body) > 1024) {
							err = errors.New("invalid worker response")
						}
						token = string(body)
					}
				}
			}
		}
		// A successful transport is insufficient: only the expected token
		// proves that the worker processed the complete order correctly.
		if err != nil || token != expected {
			code := http.StatusBadGateway
			if ctx.Err() != nil || errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded {
				code = http.StatusGatewayTimeout
			}
			http.Error(w, "worker failed or returned an incorrect result", code)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, "OK")
	})
	return mux
}

func ptrPayload(p OrderPayload) *OrderPayload { return &p }

func runGateway(protocol string, n int) {
	// The gateway must know where the separately deployed worker is listening.
	host := os.Getenv("WORKER_HOST")
	if host == "" {
		log.Fatal("WORKER_HOST is required")
	}
	client := newHTTPClient()
	defer client.CloseIdleConnections()
	var rpc pb.OrderServiceClient
	if protocol == "grpc" {
		// gRPC uses its worker port and permits messages up to the same bound as
		// HTTP request bodies.
		conn, err := grpc.Dial(net.JoinHostPort(host, "50051"), grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxBody), grpc.MaxCallSendMsgSize(maxBody)))
		if err != nil {
			log.Fatal(err)
		}
		defer conn.Close()
		rpc = pb.NewOrderServiceClient(conn)
	}
	serveHTTP(":80", gatewayHandler(protocol, host, n, client, rpc))
}

func main() {
	// PROTOCOL selects the wire format; the optional namespace prefix is
	// accepted by deployment configurations but does not change the protocol.
	protocol := strings.TrimPrefix(os.Getenv("PROTOCOL"), "ns-")
	switch protocol {
	case "json", "msgpack", "grpc", "flatbuffers":
	default:
		log.Fatal("PROTOCOL must be json, msgpack, grpc, or flatbuffers (optional ns- prefix)")
	}
	n := 10
	// PAYLOAD_SIZE controls the shared number of generated items on both roles.
	if raw := os.Getenv("PAYLOAD_SIZE"); raw != "" {
		var err error
		n, err = strconv.Atoi(raw)
		if err != nil || n < 1 || n > maxItems {
			log.Fatal("PAYLOAD_SIZE must be an integer from 1 to 10000")
		}
	}
	// The gateway is the default role so local or container launches need not
	// set SERVICE_ROLE explicitly.
	switch os.Getenv("SERVICE_ROLE") {
	case "worker":
		runWorker(protocol)
	case "gateway", "":
		runGateway(protocol, n)
	default:
		log.Fatal("SERVICE_ROLE must be worker or gateway")
	}
}
