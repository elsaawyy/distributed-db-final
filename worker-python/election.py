import random
import requests
import threading
import time
import logging

logger = logging.getLogger("worker-python")

class RaftNode:
    def __init__(self, my_addr: str, peers: list, master_addr: str):
        self.state = "follower"
        self.current_term = 0
        self.voted_for = None
        self.leader_addr = None
        self.my_addr = my_addr
        self.peers = peers
        self.master_addr = master_addr
        self.lock = threading.Lock()
    
    def start(self):
        thread = threading.Thread(target=self._election_loop, daemon=True)
        thread.start()
    
    def _is_master_alive(self) -> bool:
        try:
            resp = requests.get(f"{self.master_addr}/health", timeout=2)
            return resp.status_code == 200
        except:
            return False
    
    def _election_loop(self):
        while True:
            time.sleep(2)
            
            if self._is_master_alive():
                with self.lock:
                    if self.state != "follower":
                        self.state = "follower"
                        self.leader_addr = self.master_addr
                continue
            
            with self.lock:
                is_leader = self.state == "leader"
            
            if is_leader:
                continue
            
            if self._should_start_election():
                self._start_election()
    
    def _should_start_election(self) -> bool:
        if self.leader_addr and self.leader_addr != self.master_addr:
            try:
                resp = requests.get(f"{self.leader_addr}/health", timeout=2)
                if resp.status_code == 200:
                    return False
            except:
                pass
        return True
    
    def _start_election(self):
        with self.lock:
            self.state = "candidate"
            self.current_term += 1
            self.voted_for = self.my_addr
            term = self.current_term
        
        logger.info(f"[ELECTION] Node {self.my_addr} starting election for term {term}")
        
        votes = 1
        for peer in self.peers:
            try:
                resp = requests.post(
                    f"{peer}/vote",
                    json={"term": term, "candidateId": self.my_addr},
                    timeout=1
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("data", {}).get("voteGranted"):
                        votes += 1
            except:
                pass
        
        if votes > len(self.peers) // 2:
            with self.lock:
                self.state = "leader"
                self.leader_addr = self.my_addr
            logger.info(f"[ELECTION] Node {self.my_addr} became LEADER for term {term}")
            self._send_heartbeats()
        else:
            with self.lock:
                self.state = "follower"
            logger.info(f"[ELECTION] Node {self.my_addr} lost election")
    
    def _send_heartbeats(self):
        def heartbeat_loop():
            while True:
                time.sleep(2)
                with self.lock:
                    if self.state != "leader":
                        break
                    term = self.current_term
                    my_addr = self.my_addr
                    peers = self.peers.copy()
                
                heartbeat = {"term": term, "leader": my_addr}
                for peer in peers:
                    try:
                        requests.post(f"{peer}/heartbeat", json=heartbeat, timeout=1)
                    except:
                        pass
        
        threading.Thread(target=heartbeat_loop, daemon=True).start()
    
    def handle_vote(self, req: dict) -> dict:
        with self.lock:
            term = req.get("term", 0)
            candidate_id = req.get("candidateId", "")
            
            response = {"term": self.current_term, "voteGranted": False}
            
            if term > self.current_term:
                self.current_term = term
                self.state = "follower"
                self.voted_for = None
            
            if term == self.current_term and self.voted_for is None:
                response["voteGranted"] = True
                self.voted_for = candidate_id
            
            return response
    
    def handle_heartbeat(self, heartbeat: dict):
        with self.lock:
            term = heartbeat.get("term", 0)
            leader = heartbeat.get("leader", "")
            
            if term >= self.current_term:
                self.current_term = term
                self.state = "follower"
                self.leader_addr = leader
    
    def is_leader(self) -> bool:
        with self.lock:
            return self.state == "leader"
    
    def get_leader_addr(self) -> str:
        with self.lock:
            return self.leader_addr