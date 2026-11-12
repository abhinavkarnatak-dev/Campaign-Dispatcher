"""Run the mock provider:  python -m mock_telephony [--speed 600] [--seed 7] [--port 8099]"""
import argparse
import os

import uvicorn

from .server import create_app

HERE = os.path.dirname(os.path.abspath(__file__))


def main():
    ap = argparse.ArgumentParser(description="Agentic Universe mock telephony provider")
    ap.add_argument("--fixtures", default=os.path.join(HERE, "..", "fixtures", "campaigns.json"))
    ap.add_argument("--speed", type=float, default=600.0,
                    help="simulated seconds per real second (default 600: 1 real sec = 10 sim min)")
    ap.add_argument("--seed", type=int, default=7, help="outcome seed; graders use a different one")
    ap.add_argument("--failure-scale", type=float, default=1.0,
                    help="multiplier on injected failures (0 = none; useful while developing)")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8099)
    a = ap.parse_args()
    app = create_app(a.fixtures, a.speed, a.seed, a.failure_scale)
    print(f"Mock telephony on http://{a.host}:{a.port}  speed={a.speed}x  seed={a.seed}  "
          f"failure_scale={a.failure_scale}  docs: /docs")
    uvicorn.run(app, host=a.host, port=a.port, log_level="warning")


if __name__ == "__main__":
    main()
