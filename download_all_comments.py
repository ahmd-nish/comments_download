"""
Download all comments for community posts that have comments.

Reads post IDs from ../posts_with_comments.json and downloads comments
from the Minecraft feedback API.

Features:
  - Batch processing: writes results to disk after each batch
  - Resume support: checkpoint file tracks completed post IDs
  - Rate limiting: configurable delays + exponential backoff on 403/429
  - Per-post JSON files grouped into batch folders for easy management
  - Retries with backoff on transient errors

Usage:
    python download_all_comments.py
    python download_all_comments.py --batch-size 50 --delay 0.5
    python download_all_comments.py --reset  # clear checkpoint and restart
"""

import argparse
import json
import os
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

# ── Paths ──────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent
INPUT_FILE = SCRIPT_DIR / "posts_with_comments.json"
OUTPUT_DIR = SCRIPT_DIR / "comments_data"
CHECKPOINT_FILE = SCRIPT_DIR / "checkpoint.json"
ERRORS_LOG = SCRIPT_DIR / "errors.log"

# ── API Config ─────────────────────────────────────────────────────
BASE_URL = "https://feedback.minecraft.net"
PER_PAGE = 100
TIMEOUT = 60

# ── Defaults (overridable via CLI args) ────────────────────────────
DEFAULT_BATCH_SIZE = 25
DEFAULT_DELAY = 0.4          # seconds between requests
DEFAULT_BATCH_DELAY = 5.0    # seconds between batches
DEFAULT_MAX_RETRIES = 5
DEFAULT_BACKOFF_BASE = 10    # initial backoff seconds on 403/429


def parse_args():
    parser = argparse.ArgumentParser(description="Download comments for community posts")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
                        help=f"Posts per batch (default: {DEFAULT_BATCH_SIZE})")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY,
                        help=f"Delay between API requests in seconds (default: {DEFAULT_DELAY})")
    parser.add_argument("--batch-delay", type=float, default=DEFAULT_BATCH_DELAY,
                        help=f"Delay between batches in seconds (default: {DEFAULT_BATCH_DELAY})")
    parser.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES,
                        help=f"Max retries per request (default: {DEFAULT_MAX_RETRIES})")
    parser.add_argument("--backoff-base", type=float, default=DEFAULT_BACKOFF_BASE,
                        help=f"Base backoff seconds for 403/429 (default: {DEFAULT_BACKOFF_BASE})")
    parser.add_argument("--reset", action="store_true",
                        help="Clear checkpoint and restart from scratch")
    return parser.parse_args()


# ── Checkpoint ─────────────────────────────────────────────────────
def load_checkpoint():
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE) as f:
            return json.load(f)
    return {
        "completed_post_ids": [],
        "total_comments_downloaded": 0,
        "last_updated": None,
    }


def save_checkpoint(ckpt):
    ckpt["last_updated"] = datetime.now(timezone.utc).isoformat()
    tmp = CHECKPOINT_FILE.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(ckpt, f, indent=2)
    tmp.rename(CHECKPOINT_FILE)


def reset_checkpoint():
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()
        print("Checkpoint cleared.")


# ── Error logging ──────────────────────────────────────────────────
def log_error(post_id, error_msg):
    timestamp = datetime.now(timezone.utc).isoformat()
    with open(ERRORS_LOG, "a") as f:
        f.write(f"[{timestamp}] post_id={post_id} | {error_msg}\n")


# ── API ────────────────────────────────────────────────────────────
def download_comments_for_post(post_id, delay, max_retries, backoff_base):
    """Download all comments for a single post with pagination and retries."""
    url = (
        f"{BASE_URL}/api/v2/community/posts/{post_id}/comments.json"
        f"?per_page={PER_PAGE}&sort_by=created_at&sort_order=asc"
    )
    all_comments = []
    page = 0

    while url:
        page += 1
        success = False

        for attempt in range(1, max_retries + 1):
            try:
                resp = requests.get(url, timeout=TIMEOUT, headers={
                    "User-Agent": "MinecraftFeedbackResearch/1.0"
                })

                if resp.status_code == 200:
                    data = resp.json()
                    comments = data.get("comments", [])
                    all_comments.extend(comments)
                    url = data.get("next_page")
                    success = True
                    break

                elif resp.status_code in (403, 429):
                    # Rate limited — backoff with jitter
                    wait = backoff_base * (2 ** (attempt - 1)) + random.uniform(0, 3)
                    print(f"    [{resp.status_code}] post {post_id} page {page}, "
                          f"attempt {attempt}/{max_retries}, waiting {wait:.1f}s...")
                    time.sleep(wait)

                elif resp.status_code == 404:
                    # Post deleted or doesn't exist — skip
                    print(f"    [404] post {post_id} not found, skipping.")
                    return all_comments, "404_not_found"

                else:
                    wait = backoff_base * attempt + random.uniform(0, 2)
                    print(f"    [{resp.status_code}] post {post_id} page {page}, "
                          f"attempt {attempt}/{max_retries}, waiting {wait:.1f}s...")
                    time.sleep(wait)

            except requests.exceptions.Timeout:
                wait = backoff_base * attempt
                print(f"    [TIMEOUT] post {post_id} page {page}, "
                      f"attempt {attempt}/{max_retries}, waiting {wait:.1f}s...")
                time.sleep(wait)

            except requests.exceptions.ConnectionError:
                wait = backoff_base * (2 ** (attempt - 1)) + random.uniform(0, 5)
                print(f"    [CONN ERROR] post {post_id} page {page}, "
                      f"attempt {attempt}/{max_retries}, waiting {wait:.1f}s...")
                time.sleep(wait)

            except Exception as e:
                print(f"    [ERROR] post {post_id} page {page}: {e}")
                time.sleep(backoff_base)

        if not success:
            error_msg = f"Failed after {max_retries} attempts on page {page}"
            log_error(post_id, error_msg)
            return all_comments, f"failed_page_{page}"

        # Delay between pagination requests
        if url:
            time.sleep(delay)

    return all_comments, "ok"


# ── Batch processing ──────────────────────────────────────────────
def save_batch(batch_results, batch_num):
    """Save a batch of post comments to a JSON file."""
    batch_dir = OUTPUT_DIR / "batches"
    batch_dir.mkdir(parents=True, exist_ok=True)
    batch_file = batch_dir / f"batch_{batch_num:05d}.json"

    with open(batch_file, "w", encoding="utf-8") as f:
        json.dump(batch_results, f, indent=2, ensure_ascii=False)

    return batch_file


def print_progress(done_count, total, batch_comments, total_comments, errors_count, elapsed):
    pct = (done_count / total * 100) if total else 0
    rate = done_count / elapsed if elapsed > 0 else 0
    eta_secs = (total - done_count) / rate if rate > 0 else 0
    eta_mins = eta_secs / 60

    print(f"\r  Progress: {done_count:,}/{total:,} posts ({pct:.1f}%) | "
          f"Batch comments: {batch_comments:,} | "
          f"Total comments: {total_comments:,} | "
          f"Errors: {errors_count} | "
          f"ETA: {eta_mins:.0f} min", end="", flush=True)


# ── Main ───────────────────────────────────────────────────────────
def main():
    args = parse_args()

    if args.reset:
        reset_checkpoint()

    # Load input post IDs
    if not INPUT_FILE.exists():
        print(f"ERROR: Input file not found: {INPUT_FILE}")
        print("Run the post ID extraction script first to create posts_with_comments.json")
        sys.exit(1)

    with open(INPUT_FILE) as f:
        input_data = json.load(f)

    all_posts = input_data["posts"]
    print(f"Loaded {len(all_posts):,} posts from {INPUT_FILE.name}")

    # Load checkpoint
    ckpt = load_checkpoint()
    completed = set(ckpt["completed_post_ids"])
    total_comments = ckpt["total_comments_downloaded"]

    # Filter remaining
    remaining = [p for p in all_posts if str(p["post_id"]) not in completed]
    print(f"Already completed: {len(completed):,}")
    print(f"Remaining:         {len(remaining):,}")

    if not remaining:
        print("All posts already downloaded!")
        return

    # Setup output dir
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Calculate starting batch number
    batch_num = len(completed) // args.batch_size + 1
    errors_count = 0
    start_time = time.time()

    print(f"\nStarting download with batch_size={args.batch_size}, "
          f"delay={args.delay}s, batch_delay={args.batch_delay}s\n")

    # Process in batches
    for batch_start in range(0, len(remaining), args.batch_size):
        batch = remaining[batch_start:batch_start + args.batch_size]
        batch_results = []
        batch_comments = 0

        print(f"Batch {batch_num} ({len(batch)} posts):")

        for post_info in batch:
            post_id = str(post_info["post_id"])
            expected_comments = post_info["comment_count"]

            comments, status = download_comments_for_post(
                post_id, args.delay, args.max_retries, args.backoff_base
            )

            batch_results.append({
                "post_id": int(post_id),
                "expected_comment_count": expected_comments,
                "actual_comment_count": len(comments),
                "status": status,
                "comments": comments,
            })

            batch_comments += len(comments)
            total_comments += len(comments)

            # Update checkpoint immediately per post
            completed.add(post_id)
            ckpt["completed_post_ids"] = list(completed)
            ckpt["total_comments_downloaded"] = total_comments
            save_checkpoint(ckpt)

            if status != "ok" and status != "404_not_found":
                errors_count += 1

            elapsed = time.time() - start_time
            print_progress(len(completed), len(all_posts),
                           batch_comments, total_comments, errors_count, elapsed)

            # Delay between posts
            time.sleep(args.delay + random.uniform(0, 0.2))

        # Save batch to disk
        batch_file = save_batch(batch_results, batch_num)
        print(f"\n  -> Saved {batch_file.name} "
              f"({batch_comments:,} comments from {len(batch)} posts)")

        batch_num += 1

        # Longer pause between batches
        if batch_start + args.batch_size < len(remaining):
            jitter = random.uniform(0, 2)
            print(f"  Pausing {args.batch_delay + jitter:.1f}s before next batch...\n")
            time.sleep(args.batch_delay + jitter)

    # Final summary
    elapsed = time.time() - start_time
    print("\n" + "=" * 60)
    print("DOWNLOAD COMPLETE")
    print("=" * 60)
    print(f"  Total posts processed:    {len(completed):,}")
    print(f"  Total comments downloaded: {total_comments:,}")
    print(f"  Errors:                   {errors_count}")
    print(f"  Time elapsed:             {elapsed / 60:.1f} minutes")
    print(f"  Output directory:         {OUTPUT_DIR}")
    print(f"  Checkpoint:               {CHECKPOINT_FILE}")
    if errors_count > 0:
        print(f"  Error log:                {ERRORS_LOG}")


if __name__ == "__main__":
    main()
