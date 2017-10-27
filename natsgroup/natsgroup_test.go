
package natsgroup

import (
	"fmt"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/gnatsd/logger"
	"github.com/nats-io/gnatsd/server"
	"github.com/nats-io/go-nats"
)

// ClientPort is the default port for clients to connect
const ClientPort = 11224

// RunServer runs the NATS server in a go routine
func RunServer() *server.Server {
	return RunServerWithPorts(ClientPort)
}

// RunServerWithPorts runs the NATS server with a monitor port in a go routine
func RunServerWithPorts(cport int) *server.Server {
	// To enable debug/trace output in the NATS server,
	// flip the enableLogging flag.
	enableLogging := false

	opts := &server.Options{
		Host:   "localhost",
		Port:   cport,
		NoLog:  !enableLogging,
		NoSigs: true,
	}

	s := server.New(opts)
	if s == nil {
		panic("No NATS Server object returned.")
	}

	if enableLogging {
		l := logger.NewStdLogger(true, true, true, false, true)
		s.SetLogger(l, true, true)
	}

	// Run server in Go routine.
	go s.Start()

	end := time.Now().Add(10 * time.Second)
	for time.Now().Before(end) {
		netAddr := s.Addr()
		if netAddr == nil {
			continue
		}
		addr := s.Addr().String()
		if addr == "" {
			time.Sleep(10 * time.Millisecond)
			// Retry. We might take a little while to open a connection.
			continue
		}
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			// Retry after 50ms
			time.Sleep(50 * time.Millisecond)
			continue
		}
		_ = conn.Close() // nolint

		// Wait a bit to give a chance to the server to remove this
		// "client" from its state, which may otherwise interfere with
		// some tests.
		time.Sleep(25 * time.Millisecond)

		return s
	}
	panic("Unable to start NATS Server in Go Routine")
}

func CreateNATSConn(t *testing.T) *nats.Conn {
	nc, err := nats.Connect(fmt.Sprintf("nats://localhost:%d", ClientPort))
	if err != nil {
		t.Fatalf("Unable to connect to NATS server: %v", err)
	}
	return nc
}

func addMember(name, group string, t *testing.T) *NatsGroup {
	nc := CreateNATSConn(t)
	g, _ := CreateNatsGroup(name, group, nc, 1*time.Second, nil)
	if err := g.Join(); err != nil {
		t.Fatalf("error joining group: %v", err)
	}
	return g
}

func TestNatsGroup_Basic(t *testing.T) {
	server := RunServer()
	defer server.Shutdown()

	nc := CreateNATSConn(t)
	var memberJoined bool
	var memberLeft bool
	var wg sync.WaitGroup

	wg.Add(2)
	group, _ := CreateNatsGroup("moe", "stooges", nc, 1*time.Second, func(name, status string) {
		if status == Join && name == "larry" {
			memberJoined = true
			wg.Done()
		}
		if status == Leave && name == "larry" {
			memberLeft = true
			wg.Done()
		}

		fmt.Printf("Member %s : %s\n", name, status)
	})

	// join ourselves
	if err := group.Join(); err != nil {
		t.Fatalf("Unable to join the group: %v", err)
	}
	defer group.Leave()

	larry := addMember("larry", "stooges", t)
	larry.Leave()

	// wait for the two events in the group
	wg.Wait()

	// leave ourselves
	if err := group.Leave(); err != nil {
		t.Fatalf("Unable to leave the group.")
	}

	if !memberJoined {
		t.Fatalf("no one joined")
	}

	if !memberLeft {
		t.Fatalf("no one left")
	}
}

func TestCreateNatsGroup(t *testing.T) {
	natsConn := &nats.Conn{}
	type args struct {
		name      string
		groupname string
		nc        *nats.Conn
		hbTimeout time.Duration
		cb        MemberStatusChangeHandler
	}
	tests := []struct {
		name    string
		args    args
		want    *NatsGroup
		wantErr bool
	}{
		{name: "badname", args: args{name: "", groupname: "foo", nc: natsConn, hbTimeout: time.Second * 1, cb: nil}, want: nil, wantErr: true},
		{name: "badgroup", args: args{name: "foo", groupname: "", nc: natsConn, hbTimeout: time.Second * 1, cb: nil}, want: nil, wantErr: true},
		{name: "badconn", args: args{name: "foo", groupname: "bar", nc: nil, hbTimeout: time.Second * 1, cb: nil}, want: nil, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CreateNatsGroup(tt.args.name, tt.args.groupname, tt.args.nc, tt.args.hbTimeout, tt.args.cb)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateNatsGroup() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CreateNatsGroup() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNatsGroup_GetMembers(t *testing.T) {
	server := RunServer()
	defer server.Shutdown()

	var wg sync.WaitGroup
	nc := CreateNATSConn(t)

	group, _ := CreateNatsGroup("moe", "stooges", nc, 1*time.Second, func(name, status string) {
		fmt.Printf("Member %s : %s\n", name, status)
		wg.Done()
	})

	// join ourselves
	if err := group.Join(); err != nil {
		t.Fatalf("Unable to join the group: %v", err)
	}
	defer group.Leave()

	wg.Add(2)
	g1 := addMember("larry", "stooges", t)
	defer g1.Leave()

	g2 := addMember("curly", "stooges", t)
	defer g2.Leave()

	wg.Wait()
	members := group.GetMembers()

	membersmap := make(map[string]bool)
	for _, m := range members {
		membersmap[m] = true
		fmt.Printf("%s\n", m)
	}

	if len(members) != 3 {
		t.Fatalf("Invalid count, expected 3, received %d: %v", len(members), members)
	}
	if !membersmap["larry"] || !membersmap["moe"] || !membersmap["curly"] {
		t.Fatalf("missing a member")
	}
}

func TestNatsGroup_TestMemberTimeout(t *testing.T) {
	server := RunServer()
	defer server.Shutdown()

	var timedout bool
	var joined bool
	var left bool

	nc := CreateNATSConn(t)
	group, _ := CreateNatsGroup("moe", "stooges", nc, 250*time.Millisecond, func(name, status string) {
		if status == Timeout {
			timedout = true
		}
		if status == Join {
			joined = true
		}
		if status == Leave {
			left = true
		}
	})

	group.Join()

	nc2 := CreateNATSConn(t)
	curlysGroup, _ := CreateNatsGroup("curly", "stooges", nc2, 250*time.Millisecond, nil)
	curlysGroup.Join()

	// kill the connection so curly times out
	nc2.Close()

	time.Sleep(500 * time.Millisecond)
	if !joined || !timedout || left {
		t.Fatalf("joined = %v, timedout=%v, left=%v\n", joined, timedout, left)
	}
}

func TestNatsGroup_TestMemberWillNotTimeout(t *testing.T) {
	server := RunServer()
	defer server.Shutdown()

	var timedout bool
	var joined bool
	var left bool

	group, _ := CreateNatsGroup("moe", "stooges", CreateNATSConn(t), 250*time.Millisecond, func(name, status string) {
		if status == Timeout {
			timedout = true
		}
		if status == Join {
			joined = true
		}
		if status == Leave {
			left = true
		}
	})

	group.Join()

	nc2 := CreateNATSConn(t)
	curlysGroup, _ := CreateNatsGroup("curly", "stooges", nc2, 250*time.Millisecond, nil)
	curlysGroup.Join()

	time.Sleep(500 * time.Millisecond)
	if !joined || timedout || left {
		t.Fatalf("joined = %v, timedout=%v, left=%v\n", joined, timedout, left)
	}
}
