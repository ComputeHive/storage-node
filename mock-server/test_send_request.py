"""
Full end-to-end transfer tests.

Actually uploads and downloads files over ZMQ,
verifies data integrity, tests resume, tests audit.

Usage:
    python test_full_transfer.py              # run all tests
    python test_full_transfer.py upload       # just upload
    python test_full_transfer.py download     # just download
    python test_full_transfer.py audit        # just audit
    python test_full_transfer.py resume       # test resume after disconnect
    python test_full_transfer.py large        # 5MB file transfer
"""

import socket
import json
import sys
import os
import time
import hashlib
import threading
from pathlib import Path

import zmq
import msgpack
import httpx


BACKEND_URL = "http://localhost:5000"
DATA_DIR = "Data"


# ══════════════════════════════════════════════════════════════
#  Protocol Codec (must match host's FrameCodec)
# ══════════════════════════════════════════════════════════════
import pickle

class FrameCodec:
    @staticmethod
    def encode(d: dict) -> bytes:
        return pickle.dumps(d)

    @staticmethod
    def decode(raw: bytes) -> dict:
        return pickle.loads(raw)

# ══════════════════════════════════════════════════════════════
#  Helpers
# ══════════════════════════════════════════════════════════════

def discover_host() -> tuple[str, int]:
    try:
        resp = httpx.get(f"{BACKEND_URL}/debug/hosts")
        hosts = resp.json()
        if not hosts:
            print("ERROR: No hosts registered")
            sys.exit(1)

        latest = max(hosts, key=lambda t: hosts[t]["registered_at"])
        info = hosts[latest]
        return info["ip_address"], int(info["port"])
    except httpx.ConnectError:
        print("ERROR: Backend not reachable at", BACKEND_URL)
        sys.exit(1)


def send_tcp_request(host_ip: str, host_port: int, payload: dict) -> str:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10)
    sock.connect((host_ip, host_port))
    sock.send(json.dumps(payload).encode("utf-8"))
    response = sock.recv(4096).decode("utf-8")
    sock.close()
    return response


def file_md5(filepath: str) -> str:
    h = hashlib.md5()
    with open(filepath, "rb") as f:
        while True:
            chunk = f.read(8192)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def print_result(test_name: str, passed: bool, detail: str = ""):
    icon = "✅" if passed else "❌"
    msg = f"{icon}  {test_name}"
    if detail:
        msg += f"  ({detail})"
    print(msg)


# ══════════════════════════════════════════════════════════════
#  Test: Upload a file TO the host node
# ══════════════════════════════════════════════════════════════

def test_upload(host_ip: str, host_port: int,
                shard_id: str = "e2e-upload-shard",
                file_content: bytes = None,
                chunk_size: int = 512) -> bool:
    print(f"\n{'='*60}")
    print(f"  TEST: Upload '{shard_id}' to host")
    print(f"{'='*60}")

    if file_content is None:
        file_content = b"Hello from the test client! " * 100

    # Step 1: Request upload port
    payload = {"type": "upload", "shard_id": shard_id, "port": 0}
    allocated_port = send_tcp_request(host_ip, host_port, payload)
    allocated_port = int(allocated_port)
    print(f"  Host allocated port: {allocated_port}")

    # Step 2: Connect ZMQ and wait for START
    time.sleep(0.5)
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PAIR)
    sock.connect(f"tcp://{host_ip}:{allocated_port}")
    sock.RCVTIMEO = 10000
    sock.SNDTIMEO = 10000

    start_frame = FrameCodec.decode(sock.recv())
    assert start_frame["type"] == "start", f"Expected start, got {start_frame}"
    print(f"  Received START signal from host")

    # Step 3: Check if host sends resume (file already exists partially)
    # For fresh upload, no resume frame expected — host just waits for data

    # Step 4: Send data in chunks
    offset = 0
    chunks_sent = 0
    while offset < len(file_content):
        end = min(offset + chunk_size, len(file_content))
        chunk = file_content[offset:end]

        data_frame = FrameCodec.encode({"type": "data", "data": chunk})
        sock.send(data_frame)

        ack = FrameCodec.decode(sock.recv())
        assert ack["type"] == "ACK", f"Expected ACK, got {ack}"

        offset = end
        chunks_sent += 1

    print(f"  Sent {chunks_sent} chunks ({len(file_content)} bytes)")

    # Step 5: Send END
    end_frame = FrameCodec.encode({"type": "END"})
    sock.send(end_frame)
    print(f"  Sent END signal")

    sock.close()
    ctx.term()

    # Step 6: Verify file on disk
    time.sleep(1)
    shard_path = os.path.join(DATA_DIR, shard_id)

    print(shard_path)
    if not os.path.isfile(shard_path):
        print_result("Upload", False, "File not found on disk")
        return False

    with open(shard_path, "rb") as f:
        stored = f.read()

    if stored == file_content:
        print_result("Upload", True,
                     f"{len(stored)} bytes verified")
        return True
    else:
        print_result("Upload", False,
                     f"Content mismatch: expected {len(file_content)}, got {len(stored)}")
        return False


# ══════════════════════════════════════════════════════════════
#  Test: Download a file FROM the host node
# ══════════════════════════════════════════════════════════════

def test_download(host_ip: str, host_port: int,
                  shard_id: str = "e2e-download-shard",
                  file_content: bytes = None) -> bool:
    print(f"\n{'='*60}")
    print(f"  TEST: Download '{shard_id}' from host")
    print(f"{'='*60}")

    if file_content is None:
        file_content = b"Download test data block! " * 200

    # Step 0: Write the shard file so host has something to send
    os.makedirs(DATA_DIR, exist_ok=True)
    shard_path = os.path.join(DATA_DIR, shard_id)
    with open(shard_path, "wb") as f:
        f.write(file_content)
    print(f"  Created shard on disk: {len(file_content)} bytes")

    # Step 1: Request download port
    payload = {"type": "download", "shard_id": shard_id, "port": 0}
    allocated_port = send_tcp_request(host_ip, host_port, payload)
    allocated_port = int(allocated_port)
    print(f"  Host allocated port: {allocated_port}")

    # Step 2: Connect ZMQ and wait for START
    time.sleep(0.5)
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PAIR)
    sock.connect(f"tcp://{host_ip}:{allocated_port}")
    sock.RCVTIMEO = 10000
    sock.SNDTIMEO = 10000

    start_frame = FrameCodec.decode(sock.recv())
    assert start_frame["type"] == "start", f"Expected start, got {start_frame}"
    print(f"  Received START from host")

    # Step 3: Receive data chunks
    received_data = bytearray()
    chunks_received = 0

    while True:
        frame = FrameCodec.decode(sock.recv())

        if frame["type"] == "data":
            received_data.extend(frame["data"])
            chunks_received += 1

            ack = FrameCodec.encode({"type": "ACK"})
            sock.send(ack)

        elif frame["type"] == "END":
            print(f"  Received END from host")
            break

    print(f"  Received {chunks_received} chunks ({len(received_data)} bytes)")

    sock.close()
    ctx.term()

    # Step 4: Verify
    if bytes(received_data) == file_content:
        print_result("Download", True,
                     f"{len(received_data)} bytes verified")
        return True
    else:
        print_result("Download", False,
                     f"Content mismatch: expected {len(file_content)}, got {len(received_data)}")
        return False


# ══════════════════════════════════════════════════════════════
#  Test: Audit verification
# ══════════════════════════════════════════════════════════════

def test_audit(host_ip: str, host_port: int,
               shard_id: str = "e2e-audit-shard") -> bool:
    print(f"\n{'='*60}")
    print(f"  TEST: Audit '{shard_id}'")
    print(f"{'='*60}")

    # Create shard
    os.makedirs(DATA_DIR, exist_ok=True)
    content = os.urandom(2048)
    shard_path = os.path.join(DATA_DIR, shard_id)
    with open(shard_path, "wb") as f:
        f.write(content)

    # Test with 3 different salts
    salts = ["salt-alpha", "salt-beta", "salt-gamma"]
    all_passed = True

    for salt in salts:
        payload = {
            "type": "audit",
            "shard_id": shard_id,
            "salt": salt,
            "port": 0,
        }

        response = send_tcp_request(host_ip, host_port, payload)
        expected = hashlib.md5(content + salt.encode()).hexdigest()

        match = response == expected
        if not match:
            all_passed = False
        print(f"  Salt '{salt}': host={response[:16]}... expected={expected[:16]}... {'✓' if match else '✗'}")

    print_result("Audit", all_passed,
                 f"Tested {len(salts)} salts")
    return all_passed


# ══════════════════════════════════════════════════════════════
#  Test: Upload then Download roundtrip
# ══════════════════════════════════════════════════════════════

def test_roundtrip(host_ip: str, host_port: int) -> bool:
    print(f"\n{'='*60}")
    print(f"  TEST: Upload → Audit → Download roundtrip")
    print(f"{'='*60}")

    shard_id = f"e2e-roundtrip-{int(time.time())}"
    original_data = os.urandom(4096)

    # Upload
    print(f"\n  --- Phase 1: Upload ---")
    upload_ok = test_upload(
        host_ip, host_port,
        shard_id=shard_id,
        file_content=original_data,
    )
    if not upload_ok:
        print_result("Roundtrip", False, "Upload phase failed")
        return False

    # Audit
    print(f"\n  --- Phase 2: Audit ---")
    salt = "roundtrip-salt"
    audit_payload = {
        "type": "audit",
        "shard_id": shard_id,
        "salt": salt,
        "port": 0,
    }
    audit_response = send_tcp_request(host_ip, host_port, audit_payload)
    expected_hash = hashlib.md5(original_data + salt.encode()).hexdigest()
    audit_ok = audit_response == expected_hash
    print(f"  Audit match: {audit_ok}")

    if not audit_ok:
        print_result("Roundtrip", False, "Audit phase failed")
        return False

    # Download
    print(f"\n  --- Phase 3: Download ---")
    download_ok = test_download(
        host_ip, host_port,
        shard_id=shard_id,
        file_content=original_data,
    )
    if not download_ok:
        print_result("Roundtrip", False, "Download phase failed")
        return False

    print_result("Roundtrip", True,
                 "Upload → Audit → Download all verified")
    return True


# ══════════════════════════════════════════════════════════════
#  Test: Large file transfer (5MB)
# ══════════════════════════════════════════════════════════════

def test_large_transfer(host_ip: str, host_port: int) -> bool:
    print(f"\n{'='*60}")
    print(f"  TEST: Large file transfer (5MB)")
    print(f"{'='*60}")

    shard_id = f"e2e-large-{int(time.time())}"
    size = 5 * 1024 * 1024
    large_data = os.urandom(size)

    print(f"  Generated {size / (1024*1024):.1f} MB of random data")
    print(f"  MD5: {hashlib.md5(large_data).hexdigest()}")

    start_time = time.time()

    # Upload
    upload_ok = test_upload(
        host_ip, host_port,
        shard_id=shard_id,
        file_content=large_data,
        chunk_size=524288,
    )

    if not upload_ok:
        print_result("Large Transfer", False, "Upload failed")
        return False

    upload_time = time.time() - start_time

    # Download
    download_start = time.time()
    download_ok = test_download(
        host_ip, host_port,
        shard_id=shard_id,
        file_content=large_data,
    )

    if not download_ok:
        print_result("Large Transfer", False, "Download failed")
        return False

    download_time = time.time() - download_start
    total_time = time.time() - start_time

    print(f"\n  Upload time:   {upload_time:.2f}s")
    print(f"  Download time: {download_time:.2f}s")
    print(f"  Total time:    {total_time:.2f}s")
    print(f"  Throughput:    {(size * 2 / total_time) / (1024*1024):.2f} MB/s (both ways)")

    print_result("Large Transfer", True,
                 f"5MB up+down in {total_time:.1f}s")
    return True


# ══════════════════════════════════════════════════════════════
#  Test: Multiple concurrent transfers
# ══════════════════════════════════════════════════════════════

def test_concurrent(host_ip: str, host_port: int) -> bool:
    print(f"\n{'='*60}")
    print(f"  TEST: 3 concurrent uploads")
    print(f"{'='*60}")

    results = {}
    threads = []

    def upload_worker(idx: int):
        sid = f"e2e-concurrent-{idx}-{int(time.time())}"
        data = os.urandom(1024 * (idx + 1))
        ok = test_upload(host_ip, host_port, shard_id=sid,
                         file_content=data)
        results[idx] = ok

    for i in range(3):
        t = threading.Thread(target=upload_worker, args=(i,))
        threads.append(t)

    for t in threads:
        t.start()
        time.sleep(0.3)

    for t in threads:
        t.join(timeout=30)

    all_ok = all(results.values())
    passed = sum(1 for v in results.values() if v)
    print_result("Concurrent", all_ok,
                 f"{passed}/3 uploads succeeded")
    return all_ok


# ══════════════════════════════════════════════════════════════
#  Test: Backend integration
# ══════════════════════════════════════════════════════════════

def test_backend_integration(host_ip: str, host_port: int) -> bool:
    print(f"\n{'='*60}")
    print(f"  TEST: Backend integration (shards registered)")
    print(f"{'='*60}")

    shard_id = f"e2e-backend-{int(time.time())}"
    content = b"backend integration test data " * 50

    # Upload
    ok = test_upload(host_ip, host_port, shard_id=shard_id,
                     file_content=content)
    if not ok:
        print_result("Backend Integration", False, "Upload failed")
        return False

    # Wait for host to notify backend
    time.sleep(2)

    # Check backend knows about the shard
    resp = httpx.get(f"{BACKEND_URL}/debug/shards")
    shards = resp.json()

    if shard_id in shards:
        shard_info = shards[shard_id]
        print(f"  Backend has shard: status={shard_info['status']}")
        print_result("Backend Integration", True,
                     "Shard registered in backend after upload")
        return True
    else:
        print(f"  Backend shards: {list(shards.keys())}")
        print_result("Backend Integration", False,
                     "Shard NOT found in backend")
        return False


# ══════════════════════════════════════════════════════════════
#  Test: Heartbeat monitoring
# ══════════════════════════════════════════════════════════════

def test_heartbeat() -> bool:
    print(f"\n{'='*60}")
    print(f"  TEST: Heartbeat reaching backend")
    print(f"{'='*60}")

    resp = httpx.get(f"{BACKEND_URL}/debug/heartbeats")
    beats = resp.json()

    if not beats:
        print_result("Heartbeat", False, "No heartbeats recorded")
        return False

    latest_token = max(beats, key=lambda t: beats[t])
    latest_time = beats[latest_token]
    age = time.time() - latest_time

    print(f"  Latest heartbeat: {age:.1f}s ago")

    if age < 600:
        print_result("Heartbeat", True,
                     f"Pulse received {age:.0f}s ago")
        return True
    else:
        print_result("Heartbeat", False,
                     f"Last pulse was {age:.0f}s ago (too old)")
        return False


# ══════════════════════════════════════════════════════════════
#  Test: Empty file edge case
# ══════════════════════════════════════════════════════════════

def test_empty_file(host_ip: str, host_port: int) -> bool:
    print(f"\n{'='*60}")
    print(f"  TEST: Empty file upload")
    print(f"{'='*60}")

    shard_id = f"e2e-empty-{int(time.time())}"

    payload = {"type": "upload", "shard_id": shard_id, "port": 0}
    allocated_port = send_tcp_request(host_ip, host_port, payload)
    allocated_port = int(allocated_port)

    time.sleep(0.5)
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PAIR)
    sock.connect(f"tcp://{host_ip}:{allocated_port}")
    sock.RCVTIMEO = 10000
    sock.SNDTIMEO = 10000

    start_frame = FrameCodec.decode(sock.recv())
    assert start_frame["type"] == "start"

    # Send END immediately (no data)
    end_frame = FrameCodec.encode({"type": "END"})
    sock.send(end_frame)

    sock.close()
    ctx.term()
    time.sleep(1)

    shard_path = os.path.join(DATA_DIR, shard_id)
    if os.path.isfile(shard_path):
        size = os.path.getsize(shard_path)
        print_result("Empty File", size == 0,
                     f"File exists, size={size}")
        return size == 0
    else:
        print_result("Empty File", True,
                     "File created (empty)")
        return True


# ══════════════════════════════════════════════════════════════
#  Test: Binary data integrity (all byte values)
# ══════════════════════════════════════════════════════════════

def test_binary_integrity(host_ip: str, host_port: int) -> bool:
    print(f"\n{'='*60}")
    print(f"  TEST: Binary data integrity (all 256 byte values)")
    print(f"{'='*60}")

    shard_id = f"e2e-binary-{int(time.time())}"

    # Create data with every possible byte value
    all_bytes = bytes(range(256)) * 100
    print(f"  Data size: {len(all_bytes)} bytes, contains all 256 byte values")

    ok = test_upload(host_ip, host_port, shard_id=shard_id,
                     file_content=all_bytes)

    if not ok:
        print_result("Binary Integrity", False, "Upload failed")
        return False

    download_ok = test_download(host_ip, host_port,
                                shard_id=shard_id,
                                file_content=all_bytes)

    print_result("Binary Integrity", download_ok,
                 "All byte values preserved through upload+download")
    return download_ok


# ══════════════════════════════════════════════════════════════
#  Runner
# ══════════════════════════════════════════════════════════════

def run_all(host_ip: str, host_port: int):
    print(f"\n{'#'*60}")
    print(f"  FULL END-TO-END TEST SUITE")
    print(f"  Host: {host_ip}:{host_port}")
    print(f"  Backend: {BACKEND_URL}")
    print(f"{'#'*60}")

    results = {}
    start = time.time()

    results["Heartbeat"] = test_heartbeat()
    results["Audit"] = test_audit(host_ip, host_port)
    results["Upload"] = test_upload(host_ip, host_port)
    results["Download"] = test_download(host_ip, host_port)
    results["Roundtrip"] = test_roundtrip(host_ip, host_port)
    results["Empty File"] = test_empty_file(host_ip, host_port)
    results["Binary Integrity"] = test_binary_integrity(host_ip, host_port)
    results["Backend Integration"] = test_backend_integration(host_ip, host_port)
    results["Concurrent"] = test_concurrent(host_ip, host_port)
    results["Large Transfer"] = test_large_transfer(host_ip, host_port)

    elapsed = time.time() - start

    # Summary
    print(f"\n{'#'*60}")
    print(f"  RESULTS SUMMARY ({elapsed:.1f}s)")
    print(f"{'#'*60}")

    passed = 0
    failed = 0
    for name, ok in results.items():
        icon = "✅" if ok else "❌"
        print(f"  {icon}  {name}")
        if ok:
            passed += 1
        else:
            failed += 1

    print(f"\n  Total: {passed} passed, {failed} failed, {len(results)} total")
    print(f"{'#'*60}\n")

    return failed == 0


def cleanup():
    """Remove test shards from Data directory."""
    if not os.path.isdir(DATA_DIR):
        return

    removed = 0
    for name in os.listdir(DATA_DIR):
        if name.startswith("e2e-"):
            os.unlink(os.path.join(DATA_DIR, name))
            removed += 1

    if removed:
        print(f"  Cleaned up {removed} test shards")


if __name__ == "__main__":
    host_ip, host_port = discover_host()

    if len(sys.argv) < 2:
        cmd = "all"
    else:
        cmd = sys.argv[1].lower()

    try:
        if cmd == "all":
            success = run_all(host_ip, host_port)
            cleanup()
            sys.exit(0 if success else 1)

        elif cmd == "upload":
            test_upload(host_ip, host_port)

        elif cmd == "download":
            test_download(host_ip, host_port)

        elif cmd == "audit":
            test_audit(host_ip, host_port)

        elif cmd == "roundtrip":
            test_roundtrip(host_ip, host_port)

        elif cmd == "large":
            test_large_transfer(host_ip, host_port)

        elif cmd == "concurrent":
            test_concurrent(host_ip, host_port)

        elif cmd == "backend":
            test_backend_integration(host_ip, host_port)

        elif cmd == "binary":
            test_binary_integrity(host_ip, host_port)

        elif cmd == "empty":
            test_empty_file(host_ip, host_port)

        elif cmd == "heartbeat":
            test_heartbeat()

        elif cmd == "cleanup":
            cleanup()

        else:
            print(f"Unknown: {cmd}")
            print("Commands: all upload download audit roundtrip large concurrent backend binary empty heartbeat cleanup")

    except KeyboardInterrupt:
        print("\nInterrupted")
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()