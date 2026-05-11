package election

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"
)

type NodeState string

const (
	Follower  NodeState = "follower"
	Candidate NodeState = "candidate"
	Leader    NodeState = "leader"
)

type RaftNode struct {
	mu          sync.RWMutex
	state       NodeState
	currentTerm int
	votedFor    string
	leaderAddr  string
	myAddr      string
	peers       []string
	masterAddr  string
	heartbeatCh chan bool
}

type VoteRequest struct {
	Term        int    `json:"term"`
	CandidateId string `json:"candidateId"`
}

type VoteResponse struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"voteGranted"`
}

type Heartbeat struct {
	Term   int    `json:"term"`
	Leader string `json:"leader"`
}

func NewRaftNode(myAddr string, peers []string, masterAddr string) *RaftNode {
	return &RaftNode{
		state:       Follower,
		currentTerm: 0,
		votedFor:    "",
		leaderAddr:  "",
		myAddr:      myAddr,
		peers:       peers,
		masterAddr:  masterAddr,
		heartbeatCh: make(chan bool, 1),
	}
}

func (r *RaftNode) Start() {
	go r.electionLoop()
}

func (r *RaftNode) electionLoop() {
	for {
		time.Sleep(2 * time.Second)

		// First check if master is alive
		if r.isMasterAlive() {
			// Master is alive, I am just a follower
			r.mu.Lock()
			if r.state != Follower {
				r.state = Follower
				r.leaderAddr = r.masterAddr
			}
			r.mu.Unlock()
			continue
		}

		// Master is dead, start election among workers
		r.mu.RLock()
		isLeader := r.state == Leader
		r.mu.RUnlock()

		if isLeader {
			continue
		}

		if r.shouldStartElection() {
			r.startElection()
		}
	}
}

func (r *RaftNode) isMasterAlive() bool {
	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(r.masterAddr + "/health")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == 200
}

func (r *RaftNode) shouldStartElection() bool {
	if r.leaderAddr != "" && r.leaderAddr != r.masterAddr {
		client := http.Client{Timeout: 2 * time.Second}
		resp, err := client.Get(r.leaderAddr + "/health")
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			return false
		}
	}
	return true
}

func (r *RaftNode) startElection() {
	r.mu.Lock()
	r.state = Candidate
	r.currentTerm++
	r.votedFor = r.myAddr
	term := r.currentTerm
	r.mu.Unlock()

	log.Printf("[ELECTION] Node %s starting election for term %d", r.myAddr, term)

	votes := 1
	var muVotes sync.Mutex
	var wg sync.WaitGroup

	for _, peer := range r.peers {
		wg.Add(1)
		go func(peerAddr string) {
			defer wg.Done()

			reqBody := VoteRequest{
				Term:        term,
				CandidateId: r.myAddr,
			}
			jsonData, _ := json.Marshal(reqBody)

			client := http.Client{Timeout: 1 * time.Second}
			resp, err := client.Post(peerAddr+"/vote", "application/json", bytes.NewReader(jsonData))
			if err != nil {
				return
			}
			defer resp.Body.Close()

			var voteResp VoteResponse
			json.NewDecoder(resp.Body).Decode(&voteResp)

			if voteResp.VoteGranted {
				muVotes.Lock()
				votes++
				muVotes.Unlock()
			}
		}(peer)
	}

	wg.Wait()

	if votes > len(r.peers)/2 {
		r.mu.Lock()
		r.state = Leader
		r.leaderAddr = r.myAddr
		r.mu.Unlock()

		log.Printf("[ELECTION] Node %s became LEADER for term %d with %d votes", r.myAddr, term, votes)
		go r.sendHeartbeats()
	} else {
		r.mu.Lock()
		r.state = Follower
		r.mu.Unlock()
		log.Printf("[ELECTION] Node %s lost election for term %d", r.myAddr, term)
	}
}

func (r *RaftNode) sendHeartbeats() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		r.mu.RLock()
		isLeader := r.state == Leader
		term := r.currentTerm
		myAddr := r.myAddr
		peers := make([]string, len(r.peers))
		copy(peers, r.peers)
		r.mu.RUnlock()

		if !isLeader {
			return
		}

		heartbeat := Heartbeat{
			Term:   term,
			Leader: myAddr,
		}
		jsonData, _ := json.Marshal(heartbeat)

		for _, peer := range peers {
			go func(peerAddr string) {
				client := http.Client{Timeout: 1 * time.Second}
				client.Post(peerAddr+"/heartbeat", "application/json", bytes.NewReader(jsonData))
			}(peer)
		}
	}
}

func (r *RaftNode) HandleVote(req VoteRequest) VoteResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	response := VoteResponse{Term: r.currentTerm, VoteGranted: false}

	if req.Term > r.currentTerm {
		r.currentTerm = req.Term
		r.state = Follower
		r.votedFor = ""
	}

	if req.Term == r.currentTerm && r.votedFor == "" {
		response.VoteGranted = true
		r.votedFor = req.CandidateId
		log.Printf("[ELECTION] Node %s voted for %s", r.myAddr, req.CandidateId)
	}

	return response
}

func (r *RaftNode) HandleHeartbeat(heartbeat Heartbeat) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if heartbeat.Term >= r.currentTerm {
		r.currentTerm = heartbeat.Term
		r.state = Follower
		r.leaderAddr = heartbeat.Leader
		log.Printf("[HEARTBEAT] Node %s acknowledged leader %s", r.myAddr, heartbeat.Leader)

		select {
		case r.heartbeatCh <- true:
		default:
		}
	}
}

func (r *RaftNode) IsLeader() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state == Leader
}

func (r *RaftNode) GetLeaderAddr() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.leaderAddr
}

func (r *RaftNode) GetOtherWorkerAddr() string {
	for _, peer := range r.peers {
		if peer != r.myAddr {
			return peer
		}
	}
	return ""
}
