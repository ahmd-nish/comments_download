"""
Merge all batch JSON files into a single consolidated output file.

Run this after download_all_comments.py finishes (or at any point to get
a snapshot of what's been downloaded so far).

Usage:
    python merge_batches.py
    python merge_batches.py --output my_comments.json
"""

import argparse
import json
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
BATCH_DIR = SCRIPT_DIR / "comments_data" / "batches"
DEFAULT_OUTPUT = SCRIPT_DIR / "comments_data" / "all_comments.json"


def main():
    parser = argparse.ArgumentParser(description="Merge batch files into one JSON")
    parser.add_argument("--output", type=str, default=str(DEFAULT_OUTPUT),
                        help=f"Output file path (default: {DEFAULT_OUTPUT})")
    args = parser.parse_args()

    if not BATCH_DIR.exists():
        print(f"No batch directory found at {BATCH_DIR}")
        return

    batch_files = sorted(BATCH_DIR.glob("batch_*.json"))
    if not batch_files:
        print("No batch files found.")
        return

    print(f"Found {len(batch_files)} batch files")

    all_posts = []
    total_comments = 0
    status_counts = {}

    for bf in batch_files:
        with open(bf) as f:
            batch = json.load(f)
        for post in batch:
            all_posts.append(post)
            total_comments += post["actual_comment_count"]
            s = post["status"]
            status_counts[s] = status_counts.get(s, 0) + 1

    output = {
        "total_posts": len(all_posts),
        "total_comments": total_comments,
        "status_summary": status_counts,
        "posts": all_posts,
    }

    output_path = Path(args.output)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\nMerged {len(all_posts):,} posts with {total_comments:,} comments")
    print(f"Status breakdown: {json.dumps(status_counts, indent=2)}")
    print(f"Saved to: {output_path}")


if __name__ == "__main__":
    main()
