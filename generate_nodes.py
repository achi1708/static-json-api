#!/usr/bin/env python3
"""
Generate a large synthetic nodes dataset (100K–300K+ rows) with acyclic parent references.

Output formats:
- JSONL (default): one JSON object per line (stream-friendly for huge files)
- JSON array: a single JSON array (use --format json)

Each record schema:
{
  "timestamp": "2025-07-14T09:51:36.000Z",
  "NodeId": "00416e00-148b-49d1-ba25-ce1ecb8783b5",
  "BufferHealth": 123.45,
  "SessionId": "f4b2a2e6-5a7b-4a2a-bcf1-6e2b8e0f9c98",
  "Source": "CDN" | "<another NodeId>"
}

Key guarantees:
- NodeId, SessionId are UUID-like 36-char strings (8-4-4-4-12).
- Source is either "CDN" (root/default) or the NodeId of a previously-created node -> no cycles.
- You control scale and shape via CLI options.
"""

import argparse
import json
import math
import random
import sys
import uuid
import csv
from datetime import datetime, timedelta, timezone
from typing import List, Optional

ISO_Z = "%Y-%m-%dT%H:%M:%S.000Z"


def gen_uuid() -> str:
    # Standard UUIDv4 string format (36 chars: 8-4-4-4-12, lowercase)
    return str(uuid.uuid4())


def random_timestamp(start: datetime, end: datetime) -> str:
    # uniform timestamp between start and end
    delta = end - start
    total_seconds = delta.total_seconds()
    r = random.random()
    t = start + timedelta(seconds=r * total_seconds)
    # Zero out microseconds and force ".000Z"
    t = t.replace(microsecond=0, tzinfo=timezone.utc)
    return t.strftime(ISO_Z)


def generate_nodes(
    n: int,
    root_share: float = 0.1,
    max_children_hint: int = 6,
    buffer_min: float = 0.0,
    buffer_max: float = 200.0,
    start_ts: str = "2025-01-01T00:00:00.000Z",
    end_ts: str = "2025-12-31T23:59:59.000Z",
    max_level_tree: int = 100,
    seed: Optional[int] = None,
):
    """
    Generate nodes with acyclic parent references.
    Strategy:
    - Create nodes sequentially. For node i>0, choose Source="CDN" with probability `root_share`,
      else pick a parent uniformly from nodes [0..i-1]. This ensures no cycles by construction.
    - `max_children_hint` lightly influences the chance to choose "CDN" vs a parent by adapting
      root_share as the graph grows; keeping it simple here for performance and clarity.
    """
    if seed is not None:
        random.seed(seed)

    try:
        start_dt = datetime.strptime(start_ts, ISO_Z).replace(tzinfo=timezone.utc)
        end_dt = datetime.strptime(end_ts, ISO_Z).replace(tzinfo=timezone.utc)
    except ValueError:
        raise SystemExit("Invalid timestamp format. Use ISO like 2025-07-14T09:51:36.000Z")

    if end_dt <= start_dt:
        raise SystemExit("--end-ts must be after --start-ts")

    nodes = []
    nodes_append = nodes.append

    notes_under_max_level = []
    notes_under_max_level_append = notes_under_max_level.append

    for i in range(n):
        node_id = gen_uuid()
        session_id = gen_uuid()
        # buffer health uniform float; keep 2 decimals for readability while still float
        buffer_health = round(random.uniform(buffer_min, buffer_max), 2)
        ts = random_timestamp(start_dt, end_dt)
        level = 1

        if i == 0 or random.random() < root_share:
            source = "CDN"
        else:
            # Choose a parent from previously created nodes (acyclic guarantee)
            parent_index = random.randint(0, i - 1)
            if nodes[parent_index]["level"] <= max_level_tree - 1:
                level = nodes[parent_index]["level"] + 1
                source = nodes[parent_index]["NodeId"]
            else:
                node_under_max_level_rdm = random.choice(notes_under_max_level)
                level = node_under_max_level_rdm["level"] + 1
                source = node_under_max_level_rdm["NodeId"]

        to_append = {
            "timestamp": ts,
            "NodeId": node_id,
            "BufferHealth": buffer_health,
            "SessionId": session_id,
            "Source": source,
            "level": level,
        }

        nodes_append(to_append)

        if level < max_level_tree:
            notes_under_max_level_append(to_append)

    return nodes


def write_jsonl(path: str, rows: List[dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_json_array(path: str, rows: List[dict]) -> None:
    # Stream-friendly array writing for large lists without holding a giant string.
    with open(path, "w", encoding="utf-8") as f:
        f.write("[\n")
        first = True
        for row in rows:
            if not first:
                f.write(",\n")
            json.dump(row, f, ensure_ascii=False)
            first = False
        f.write("\n]\n")


def json_array_to_csv(infile, outfile):
    """Convert JSON array (loaded fully) to CSV."""
    with open(infile, "r", encoding="utf-8") as fin:
        data = json.load(fin)

    if not data:
        print("⚠️ No rows found")
        return

    headers_clue = ["timestamp","NodeId","BufferHealth","SessionId","Source"]
    fieldnames = sorted({k for row in data for k in row.keys() if k in headers_clue})
    data_filtered = json.loads(json.dumps([{key: x[key] for key in fieldnames} for x in data]))
    with open(outfile, "w", newline="", encoding="utf-8") as fout:
        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data_filtered)

def stream_jsonl_to_csv(infile, outfile):
    """Convert JSONL to CSV line by line (streaming)."""
    with open(infile, "r", encoding="utf-8") as fin, open(outfile, "w", newline="", encoding="utf-8") as fout:
        writer = None
        for line in fin:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)

            if writer is None:
                # Use keys from the first object
                fieldnames = list(obj.keys())
                writer = csv.DictWriter(fout, fieldnames=fieldnames)
                writer.writeheader()

            writer.writerow(obj)


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Generate large JSON nodes dataset (acyclic).")
    p.add_argument("--rows", type=int, required=True, help="Number of rows to generate (e.g., 100000)")
    p.add_argument(
        "--format",
        choices=["jsonl", "json"],
        default="jsonl",
        help="Output format: jsonl (one object per line) or json (array). Default: jsonl",
    )
    p.add_argument("--out", type=str, required=True, help="Output file path")
    p.add_argument("--root-share", type=float, default=0.1, help="Probability a node uses Source='CDN' (roots).")
    p.add_argument("--max-children-hint", type=int, default=6, help="Not strict; influences structure slightly.")
    p.add_argument("--buffer-min", type=float, default=0.0, help="Min BufferHealth (inclusive)")
    p.add_argument("--buffer-max", type=float, default=200.0, help="Max BufferHealth (inclusive)")
    p.add_argument("--start-ts", type=str, default="2025-01-01T00:00:00.000Z", help="Earliest timestamp (ISO Z)")
    p.add_argument("--end-ts", type=str, default="2025-12-31T23:59:59.000Z", help="Latest timestamp (ISO Z)")
    p.add_argument("--max-level-tree", type=int, default=100, help="Max levels of nodes in the tree")
    p.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility")
    p.add_argument("--csv-out", type=str, default='', help="Output CSV file path")

    args = p.parse_args(argv)

    if args.rows <= 0:
        raise SystemExit("--rows must be positive")
    if not (0.0 <= args.root_share <= 1.0):
        raise SystemExit("--root-share must be between 0 and 1")
    if args.buffer_min > args.buffer_max:
        raise SystemExit("--buffer-min cannot exceed --buffer-max")

    rows = generate_nodes(
        n=args.rows,
        root_share=args.root_share,
        max_children_hint=args.max_children_hint,
        buffer_min=args.buffer_min,
        buffer_max=args.buffer_max,
        start_ts=args.start_ts,
        end_ts=args.end_ts,
        max_level_tree=args.max_level_tree,
        seed=args.seed,
    )

    if args.format == "jsonl":
        write_jsonl(args.out, rows)
    else:
        write_json_array(args.out, rows)

    print(f"✅ Wrote {args.rows} rows to {args.out} as {args.format.upper()}")

    if args.csv_out != '':
        with open(args.out, "r", encoding="utf-8") as f:
            first_char = f.read(1)
            f.seek(0)

            if first_char == "[":
                print("📦 Detected JSON array format (loads full file in memory).")
                json_array_to_csv(args.out, args.csv_out)
            else:
                print("📦 Detected JSONL format (streaming line by line).")
                stream_jsonl_to_csv(args.out, args.csv_out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
