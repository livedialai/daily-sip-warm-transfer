#!/usr/bin/env python3
"""Send one raw SIP INVITE and print every reply until a final response arrives.

Useful to find out *why* a carrier rejects a call, without a softphone and
without involving Daily:

    python sip_probe.py <host> <port> <uri-user> [--codec pcma] [--from 1000]

Backed by the observations in README.md: the same codec is accepted in an
RTP/AVP offer and rejected with 488 in an RTP/AVP**F** offer.
"""
import argparse
import random
import socket
import time

CODECS = {
    "pcma": (8, "PCMA/8000"),
    "pcmu": (0, "PCMU/8000"),
    "g722": (9, "G722/8000"),
}


def build(host, port, user, codec, from_user, domain, call_id, branch, tag, sdp_port):
    pt, name = CODECS[codec]
    sdp = (
        "v=0\r\n"
        f"o=- {random.randint(1, 999999)} 1 IN IP4 127.0.0.1\r\n"
        "s=-\r\n"
        "c=IN IP4 127.0.0.1\r\n"
        "t=0 0\r\n"
        f"m=audio {sdp_port} RTP/AVP {pt} 101\r\n"
        f"a=rtpmap:{pt} {name}\r\n"
        "a=rtpmap:101 telephone-event/8000\r\n"
        "a=ptime:20\r\n"
        "a=sendrecv\r\n"
    )
    headers = [
        f"INVITE sip:{user}@{domain} SIP/2.0",
        f"Via: SIP/2.0/UDP 127.0.0.1:{sdp_port};branch={branch};rport",
        "Max-Forwards: 70",
        f"From: <sip:{from_user}@{domain}>;tag={tag}",
        f"To: <sip:{user}@{domain}>",
        f"Call-ID: {call_id}",
        "CSeq: 1 INVITE",
        f"Contact: <sip:{from_user}@127.0.0.1:{sdp_port}>",
        "Content-Type: application/sdp",
        f"Content-Length: {len(sdp)}",
        "",
        "",
    ]
    return ("\r\n".join(headers) + sdp).encode()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("host")
    ap.add_argument("port", type=int)
    ap.add_argument("user", help="called number / user part")
    ap.add_argument("--codec", default="pcma", choices=sorted(CODECS))
    ap.add_argument("--from", dest="from_user", default="1000")
    ap.add_argument("--domain", default=None, help="SIP domain (default: host)")
    ap.add_argument("--timeout", type=float, default=8.0)
    args = ap.parse_args()

    domain = args.domain or args.host
    call_id = f"{random.randint(10**8, 10**9)}@{args.host}"
    branch = f"z9hG4bK{random.randint(10**6, 10**7)}"
    tag = f"{random.randint(10**4, 10**5)}"
    sdp_port = random.randint(20000, 30000)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", sdp_port))
    sock.settimeout(1.5)
    msg = build(args.host, args.port, args.user, args.codec, args.from_user,
                domain, call_id, branch, tag, sdp_port)
    sock.sendto(msg, (args.host, args.port))
    print(f"-> INVITE {args.user}@{domain} via {args.host}:{args.port} codec={args.codec}")

    final = ""
    deadline = time.time() + args.timeout
    while time.time() < deadline:
        try:
            data, _ = sock.recvfrom(65535)
        except socket.timeout:
            continue
        line = data.decode(errors="replace").split("\r\n")[0]
        if not line.startswith("SIP/2.0"):
            continue
        print("<-", line)
        if int(line.split()[1]) >= 200:
            final = line
            break

    if final and int(final.split()[1]) < 300:
        # call is ringing / answered -> do not leave a dangling leg behind
        sock.sendto(
            (
                f"CANCEL sip:{args.user}@{domain} SIP/2.0\r\n"
                f"Via: SIP/2.0/UDP 127.0.0.1:{sdp_port};branch={branch};rport\r\n"
                f"From: <sip:{args.from_user}@{domain}>;tag={tag}\r\n"
                f"To: <sip:{args.user}@{domain}>\r\n"
                f"Call-ID: {call_id}\r\n"
                "CSeq: 1 CANCEL\r\nMax-Forwards: 70\r\nContent-Length: 0\r\n\r\n"
            ).encode(),
            (args.host, args.port),
        )
    elif not final:
        print("(no final response)")
    sock.close()


if __name__ == "__main__":
    main()
