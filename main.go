package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/vmihailenco/msgpack/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	// Replace "protocol-benchmark/pb" with your module name defined in go.mod
	pb "protocol-benchmark/pb"
)

// Standard Data Schema across all 5 protocols
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

func generateDummyPayload() OrderPayload {
	count := 10 // Default to v1.0 (10 items)
	if val := os.Getenv("PAYLOAD_SIZE"); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil {
			count = parsed
		}
	}

	items := make([]OrderItem, count)
	for i := 0; i < count; i++ {
		items[i] = OrderItem{
			ItemID:   fmt.Sprintf("item-%d", i),
			Quantity: int32(i + 1),
			Price:    19.99 * float64(i+1),
		}
	}
	return OrderPayload{
		OrderID:     "ord-123456789",
		UserID:      "usr-987654321",
		Items:       items,
		TotalAmount: 1099.45,
	}
}

// -------------------------------------------------------------------
// gRPC SERVICE IMPLEMENTATION
// -------------------------------------------------------------------
type grpcOrderServer struct {
	pb.UnimplementedOrderServiceServer
}

func (s *grpcOrderServer) ProcessOrder(ctx context.Context, req *pb.OrderPayload) (*pb.OrderResponse, error) {
	// req is automatically unmarshaled from Protobuf wire format by gRPC runtime
	return &pb.OrderResponse{Status: "OK"}, nil
}

// -------------------------------------------------------------------
// WORKER SERVICE
// -------------------------------------------------------------------
func runWorker() {
	protocol := os.Getenv("PROTOCOL")
	log.Printf("Starting Worker Service for protocol: %s", protocol)

	switch protocol {
	case "ns-json", "json":
		http.HandleFunc("/process", func(w http.ResponseWriter, r *http.Request) {
			var payload OrderPayload
			_ = json.NewDecoder(r.Body).Decode(&payload)
			w.WriteHeader(http.StatusOK)
		})
		log.Fatal(http.ListenAndServe(":8080", nil))

	case "ns-msgpack", "msgpack":
		http.HandleFunc("/process", func(w http.ResponseWriter, r *http.Request) {
			var payload OrderPayload
			body, _ := io.ReadAll(r.Body)
			_ = msgpack.Unmarshal(body, &payload)
			w.WriteHeader(http.StatusOK)
		})
		log.Fatal(http.ListenAndServe(":8080", nil))

	case "ns-grpc", "grpc":
		lis, err := net.Listen("tcp", ":50051")
		if err != nil {
			log.Fatalf("Failed to listen: %v", err)
		}
		s := grpc.NewServer()
		pb.RegisterOrderServiceServer(s, &grpcOrderServer{})
		log.Printf("gRPC Worker listening on :50051")
		log.Fatal(s.Serve(lis))

	case "ns-thrift", "thrift":
		lis, err := net.Listen("tcp", ":9090")
		if err != nil {
			log.Fatalf("Failed to listen: %v", err)
		}
		for {
			conn, err := lis.Accept()
			if err != nil {
				continue
			}
			go func(c net.Conn) {
				defer c.Close()
				var length int32
				_ = binary.Read(c, binary.BigEndian, &length)
				buf := make([]byte, length)
				_, _ = io.ReadFull(c, buf)
			}(conn)
		}

	case "ns-flatbuffers", "flatbuffers":
		http.HandleFunc("/process", func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			if len(body) >= 4 {
				// Zero-Copy access: Reading offset directly without object allocation
				_ = flatbuffers.GetUOffsetT(body)
			}
			w.WriteHeader(http.StatusOK)
		})
		log.Fatal(http.ListenAndServe(":8080", nil))
	}
}

// -------------------------------------------------------------------
// GATEWAY SERVICE
// -------------------------------------------------------------------
func runGateway() {
	protocol := os.Getenv("PROTOCOL")
	workerHost := os.Getenv("WORKER_HOST")
	log.Printf("Starting Gateway Service targeting %s via %s", workerHost, protocol)

	// Pre-establish gRPC client connection if using ns-grpc
	var grpcClient pb.OrderServiceClient
	if protocol == "ns-grpc" || protocol == "grpc" {
		conn, err := grpc.Dial(
			fmt.Sprintf("%s:50051", workerHost),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatalf("Failed to connect to gRPC worker: %v", err)
		}
		defer conn.Close()
		grpcClient = pb.NewOrderServiceClient(conn)
	}

	http.HandleFunc("/checkout", func(w http.ResponseWriter, r *http.Request) {
		dummyData := generateDummyPayload()

		switch protocol {
		case "ns-json", "json":
			body, _ := json.Marshal(dummyData)
			resp, err := http.Post(fmt.Sprintf("http://%s:8080/process", workerHost), "application/json", bytes.NewBuffer(body))
			if err == nil {
				resp.Body.Close()
			}

		case "ns-msgpack", "msgpack":
			body, _ := msgpack.Marshal(dummyData)
			resp, err := http.Post(fmt.Sprintf("http://%s:8080/process", workerHost), "application/x-msgpack", bytes.NewBuffer(body))
			if err == nil {
				resp.Body.Close()
			}

		case "ns-grpc", "grpc":
			// Convert native OrderPayload to generated Protobuf message
			pbItems := make([]*pb.OrderItem, len(dummyData.Items))
			for i, item := range dummyData.Items {
				pbItems[i] = &pb.OrderItem{
					ItemId:   item.ItemID,
					Quantity: item.Quantity,
					Price:    item.Price,
				}
			}
			pbPayload := &pb.OrderPayload{
				OrderId:     dummyData.OrderID,
				UserId:      dummyData.UserID,
				Items:       pbItems,
				TotalAmount: dummyData.TotalAmount,
			}

			// Invoke gRPC RPC method over HTTP/2
			_, _ = grpcClient.ProcessOrder(r.Context(), pbPayload)

		case "ns-thrift", "thrift":
			body, _ := msgpack.Marshal(dummyData)
			conn, err := net.Dial("tcp", fmt.Sprintf("%s:9090", workerHost))
			if err == nil {
				_ = binary.Write(conn, binary.BigEndian, int32(len(body)))
				_, _ = conn.Write(body)
				conn.Close()
			}

		case "ns-flatbuffers", "flatbuffers":
			builder := flatbuffers.NewBuilder(1024)
			orderOffset := builder.CreateString(dummyData.OrderID)
			builder.Finish(orderOffset)
			buf := builder.FinishedBytes()

			resp, err := http.Post(fmt.Sprintf("http://%s:8080/process", workerHost), "application/octet-stream", bytes.NewBuffer(buf))
			if err == nil {
				resp.Body.Close()
			}
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	log.Fatal(http.ListenAndServe(":80", nil))
}

func main() {
	role := os.Getenv("SERVICE_ROLE")
	if role == "worker" {
		runWorker()
	} else {
		runGateway()
	}
}
