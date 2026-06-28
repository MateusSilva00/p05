package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	pb "client/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var nodeAddresses = []string{
	"node_1:50051",
	"node_2:50051",
	"node_3:50051",
	"node_4:50051",
}

type RaftClient struct {
	addresses  []string
	leaderAddr string
	conn       *grpc.ClientConn
	kv         pb.KVServiceClient
}

func NewRaftClient(addresses []string) *RaftClient {
	return &RaftClient{addresses: addresses}
}

func main() {
	client := NewRaftClient(nodeAddresses)
	client.Run()
}

func (c *RaftClient) Run() {
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("╔══════════════════════════════════════╗")
	fmt.Println("║       RAFT CLIENT (Go + gRPC)        ║")
	fmt.Println("╠══════════════════════════════════════╣")
	fmt.Println("║  publish <data>  → write data        ║")
	fmt.Println("║  consume         → read all data     ║")
	fmt.Println("║  exit            → quit               ║")
	fmt.Println("╚══════════════════════════════════════╝")

	for {
		fmt.Print("raft> ")
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, " ", 2)
		cmd := strings.ToLower(parts[0])

		switch cmd {
		case "exit":
			fmt.Println("Bye!")
			return
		case "publish":
			if len(parts) < 2 {
				fmt.Println("Usage: publish <data>")
				continue
			}
			c.publish(parts[1])
		case "consume":
			c.consume()
		default:
			fmt.Println("Unknown command. Use: publish <data> | consume | exit")
		}
	}
}

// ── Connection ─────────────────────────────────────────────────────

func (c *RaftClient) connect(addr string) error {
	if c.conn != nil {
		c.conn.Close()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return err
	}

	c.conn = conn
	c.kv = pb.NewKVServiceClient(conn)
	c.leaderAddr = addr
	return nil
}

func (c *RaftClient) ensureConnected() error {
	if c.kv != nil {
		return nil
	}
	for _, addr := range c.addresses {
		if err := c.connect(addr); err == nil {
			fmt.Printf("Connected to %s\n", addr)
			return nil
		}
	}
	return fmt.Errorf("no node reachable")
}

func (c *RaftClient) disconnect() {
	if c.conn != nil {
		c.conn.Close()
	}
	c.conn = nil
	c.kv = nil
	c.leaderAddr = ""
}

// ── Publish ────────────────────────────────────────────────────────

func (c *RaftClient) publish(data string) {
	if err := c.ensureConnected(); err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	for attempts := 0; attempts < len(c.addresses); attempts++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := c.kv.Publish(ctx, &pb.PublishRequest{Data: data})
		cancel()

		if err != nil {
			fmt.Printf("RPC error: %v — reconnecting...\n", err)
			c.disconnect()
			if connErr := c.ensureConnected(); connErr != nil {
				fmt.Printf("Error: %v\n", connErr)
				return
			}
			continue
		}

		if resp.Success {
			fmt.Println("OK — data published and committed")
			return
		}

		if resp.Error == "not_leader" && resp.LeaderId != "" {
			leaderAddr := resp.LeaderId + ":50051"
			fmt.Printf("Redirecting to leader: %s\n", resp.LeaderId)
			if err := c.connect(leaderAddr); err == nil {
				continue
			}
			fmt.Printf("Failed to connect to leader %s\n", leaderAddr)
		}

		fmt.Printf("Error: %s\n", resp.Error)
		return
	}
	fmt.Println("Failed to publish after retries")
}

// ── Consume ────────────────────────────────────────────────────────

func (c *RaftClient) consume() {
	if err := c.ensureConnected(); err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	resp, err := c.kv.Consume(ctx, &pb.ConsumeRequest{})
	cancel()

	if err != nil {
		fmt.Printf("RPC error: %v\n", err)
		c.disconnect()
		return
	}

	if !resp.Success {
		fmt.Printf("Error: %s\n", resp.Error)
		return
	}

	if len(resp.Data) == 0 {
		fmt.Println("(no committed data)")
		return
	}

	for i, d := range resp.Data {
		fmt.Printf("[%d] %s\n", i+1, d)
	}
}
