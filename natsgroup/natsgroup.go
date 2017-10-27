
// Package natsgroup is a simple group membership API.
package natsgroup

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/go-nats"
)

// The heartbeat that tracks clients
type natsGroupHB struct {
	ID     string // json:id
	Name   string // json:name
	Status string // json:status
}

// Status updates
const (
	Join    = "joined"
	Leave   = "left"
	Timeout = "timeout"
	OK      = "ok"
)

// natsGroupMember represents a member of a NATS group
type natsGroupMember struct {
	name  string // a friendly name to use
	id    string // a generated ID
	timer *time.Timer
}

// MemberStatusChangeHandler is invoked upon member changes
type MemberStatusChangeHandler func(name, status string)

// NatsGroup provides a list of requests
type NatsGroup struct {
	sync.Mutex
	nc          *nats.Conn
	members     map[string]*natsGroupMember
	myID        string
	myName      string
	hbSubject   string
	hbSub       *nats.Subscription
	hbTimeout   time.Duration
	hbSendTimer *time.Timer
	mCb         MemberStatusChangeHandler
}

// CreateNatsGroup creates a NATS group
func CreateNatsGroup(name, groupname string, nc *nats.Conn, hbTimeout time.Duration, cb MemberStatusChangeHandler) (*NatsGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("natgroup: invalid name")
	}
	if groupname == "" {
		return nil, fmt.Errorf("natsgroup:  invalid groupname")
	}
	if nc == nil {
		return nil, fmt.Errorf("natgroup: invalid NATS conn")
	}
	return &NatsGroup{
		nc:        nc,
		members:   make(map[string]*natsGroupMember),
		hbSubject: fmt.Sprintf("_NATSGROUP.%s", groupname),
		myID:      name,
		myName:    name,
		hbTimeout: hbTimeout,
		mCb:       cb}, nil
}

func (ng *NatsGroup) invokeCb(id, name, status string) {
	if ng.mCb != nil && ng.myID != id {
		// potential ordering issues users based on gooutine scheduling.
		// TODO:  To solve those, have a single go routine invoke callbacks
		// pushed into a channel.
		go ng.mCb(name, status)
	}
}

// addMember creates and adds a member
func (ng *NatsGroup) addMember(name, id, status string) {

	// if we happen to catch the last HB of a member, just ignore.
	if status == Leave {
		return
	}

	ngm := &natsGroupMember{name: name, id: id}
	ng.members[id] = ngm
	ng.invokeCb(ngm.id, ngm.name, Join)

	ngm.timer = time.AfterFunc(ng.hbTimeout,
		func() {
			ng.Lock()
			delete(ng.members, ngm.id)
			ng.invokeCb(ngm.id, ngm.name, Timeout)
			ng.Unlock()
		})
}

func (ng *NatsGroup) updateMember(member *natsGroupMember, status string) {
	// keep alive this member, and reduce the chances of a timeout and
	// leave callback colliding
	member.timer.Reset(ng.hbTimeout)

	// a member is letting us know they've left
	if status == Leave {
		// remove the member
		member.timer.Stop()
		ng.invokeCb(member.id, member.name, Leave)
		return
	}
}

// processMemberHB processes a heartbeat from a member.  Caller must lock.
func (ng *NatsGroup) processMemberHb(hb *natsGroupHB) {
	ng.Lock()
	defer ng.Unlock()

	member := ng.members[hb.ID]

	// A new member has been discovered!
	if member == nil {
		ng.addMember(hb.Name, hb.ID, hb.Status)
	} else {
		ng.updateMember(member, hb.Status)
	}
}

// handles heartbeats from group members (including ourselves)
func (ng *NatsGroup) hbHandler(msg *nats.Msg) {
	var hb natsGroupHB
	if err := json.Unmarshal(msg.Data, &hb); err != nil {
		fmt.Printf("natsgroup: error unmarshalling hb.")
		return
	}

	ng.processMemberHb(&hb)
}

// isJoined keys off the hearbeat subscription
func (ng *NatsGroup) hasJoined() bool {
	return ng.hbSub != nil
}

// Join a NATS group
func (ng *NatsGroup) Join() error {
	var err error

	ng.Lock()
	defer ng.Unlock()

	if ng.hbSub == nil {
		ng.hbSub, err = ng.nc.Subscribe(ng.hbSubject, ng.hbHandler)
		if err != nil {
			return fmt.Errorf("natsgroup error subscribing:  %v", err)
		}
	}
	if err = ng.sendHB(Join); err != nil {
		return fmt.Errorf("natsgroup error join heartbeat:  %v", err)
	}

	// try to miss at least two heartbeats before timing out on the other side.
	hbFreq := ng.hbTimeout/2 - (ng.hbTimeout / 20)
	ng.hbSendTimer = time.AfterFunc(hbFreq,
		func() {
			if hbErr := ng.sendHB(OK); hbErr != nil {
				fmt.Printf("natsgroup:  unable to send heartbeat:  %v", hbErr)
			}
			ng.hbSendTimer.Reset(hbFreq)
		})
	return err
}

// GetMembers retrieves known NATS members
func (ng *NatsGroup) GetMembers() []string {
	ng.Lock()
	defer ng.Unlock()

	rv := make([]string, len(ng.members))
	i := 0
	for name := range ng.members {
		rv[i] = name
		i++
	}
	return rv
}

// Leave a NATS group
func (ng *NatsGroup) Leave() error {
	ng.Lock()
	defer ng.Unlock()

	if !ng.hasJoined() {
		return nil
	}

	fmt.Printf("members = %v\n", ng.members)
	for _, m := range ng.members {
		fmt.Printf("leaving - m = %s:%s\n", m.id, m.name)
		m.timer.Stop()
	}
	ng.hbSendTimer.Stop()

	if ng.hbSub != nil {
		sub := ng.hbSub
		ng.hbSub = nil
		if err := sub.Unsubscribe(); err != nil {
			return fmt.Errorf("natsgroup:  unable to unsubscribe:  %v", err)
		}
	}
	if err := ng.sendHB(Leave); err != nil {
		return fmt.Errorf("natsgroup error leaving:  %v", err)
	}

	return nil
}

// send a heartbeat
func (ng *NatsGroup) sendHB(status string) error {
	var payload []byte
	hb := &natsGroupHB{ID: ng.myID, Name: ng.myName, Status: status}
	payload, err := json.Marshal(hb)
	if err != nil {
		return fmt.Errorf("natsgroup error sending hb:  %v", err)
	}
	err = ng.nc.Publish(ng.hbSubject, payload)
	if err != nil {
		return fmt.Errorf("natsgroup error sending hb:  %v", err)
	}

	return nil
}
