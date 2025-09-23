#!/usr/bin/env python3
"""
StreamPro synthetic data generator.

Outputs into a target data directory:
  - users.csv
  - videos.csv
  - devices.csv
  - events.jsonl (NDJSON)

Notes:
  - All comments and strings are in English per project conventions.
  - Designed for local development; no external services required.
"""
from __future__ import annotations

import argparse
import json
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd


@dataclass
class GeneratorConfig:
    out_dir: Path
    days: int = 7
    users: int = 300
    videos: int = 80
    seed: int = 42


def ensure_out(out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)


def generate_users(cfg: GeneratorConfig) -> pd.DataFrame:
    today = datetime.utcnow().date()
    users: List[List[str]] = []
    for i in range(cfg.users):
        user_id = f"u_{i:05d}"
        signup_date = today - timedelta(days=random.randint(1, cfg.days + 7))
        subscription_tier = random.choice(["free", "basic", "premium"])
        age_group = random.choice(["18-24", "25-34", "35-44", "45-54", "55+"])
        gender = random.choice(["female", "male", "other", "prefer_not_to_say"])
        users.append([user_id, str(signup_date), subscription_tier, age_group, gender])
    return pd.DataFrame(
        users, columns=["user_id", "signup_date", "subscription_tier", "age_group", "gender"]
    )


def generate_videos(cfg: GeneratorConfig) -> pd.DataFrame:
    genres = [
        "drama",
        "comedy",
        "documentary",
        "action",
        "horror",
        "sci-fi",
        "fantasy",
        "romance",
    ]
    videos: List[List[object]] = []
    for i in range(cfg.videos):
        video_id = f"v_{i:05d}"
        title = f"Video {i}"
        genre = random.choice(genres)
        duration_seconds = random.randint(30, 3600)
        patent_id = f"pat_{random.randint(1000, 9999)}"
        videos.append([video_id, title, genre, duration_seconds, patent_id])
    return pd.DataFrame(
        videos, columns=["video_id", "title", "genre", "duration_seconds", "patent_id"]
    )


def generate_devices() -> pd.DataFrame:
    rows: List[List[str]] = []
    device_types = ["mobile", "tablet", "desktop"]
    device_models = ["A1", "A2", "B1", "B2", "C1"]
    os_versions = ["iOS 16", "Android 13", "Windows 11", "macOS 14"]
    for d in device_types:
        for m in device_models:
            for o in os_versions:
                rows.append([d, m, o])
    return pd.DataFrame(rows, columns=["device", "device_model", "os_version"])


def generate_events(cfg: GeneratorConfig, users_df: pd.DataFrame, videos_df: pd.DataFrame) -> List[dict]:
    accounts = ["acct_1", "acct_2", "acct_3"]
    device_types = ["mobile", "tablet", "desktop"]
    device_os_options = ["iOS", "Android", "Windows", "macOS"]
    app_versions = ["1.0.0", "1.1.0", "1.2.0", "2.0.0", "2.1.0"]
    network_types = ["wifi", "4G", "5G"]
    countries = ["US", "CA", "GB", "DE", "FR", "BR", "IN", "AU", "JP"]

    base_dt = datetime.utcnow() - timedelta(days=cfg.days)
    rows: List[dict] = []

    user_ids = users_df["user_id"].tolist()
    video_ids = videos_df["video_id"].tolist()

    for user_id in user_ids:
        # Draw sessions per user across the window, at least 1
        num_sessions = np.random.poisson(lam=1.8) + 1
        session_time = base_dt + timedelta(hours=random.randint(0, cfg.days * 24))
        first_login_emitted = False

        for s in range(num_sessions):
            account_id = random.choice(accounts)
            session_id = f"s_{user_id}_{s:04d}"
            device = random.choice(device_types)
            device_os = random.choice(device_os_options)
            app_version = random.choice(app_versions)
            network_type = random.choice(network_types)
            ip = f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"
            country = random.choice(countries)

            if not first_login_emitted:
                rows.append({
                    "timestamp": (session_time - timedelta(minutes=5)).isoformat(),
                    "account_id": account_id,
                    "user_id": user_id,
                    "video_id": None,
                    "session_id": session_id,
                    "event_name": "first_login",
                    "value": None,
                    "device": device,
                    "device_os": device_os,
                    "app_version": app_version,
                    "network_type": network_type,
                    "ip": ip,
                    "country": country,
                })
                first_login_emitted = True

            rows.append({
                "timestamp": session_time.isoformat(),
                "account_id": account_id,
                "user_id": user_id,
                "video_id": None,
                "session_id": session_id,
                "event_name": "session_start",
                "value": None,
                "device": device,
                "device_os": device_os,
                "app_version": app_version,
                "network_type": network_type,
                "ip": ip,
                "country": country,
            })

            # Simulate watch_time chunks
            session_watch_seconds = 0
            num_chunks = random.randint(1, 10)
            t = session_time
            watched_videos = random.sample(video_ids, k=min(len(video_ids), random.randint(1, 4)))
            for _ in range(num_chunks):
                t += timedelta(seconds=random.randint(1, 50))
                value = int(np.random.exponential(scale=12)) + 1  # skewed distribution
                session_watch_seconds += value
                rows.append({
                    "timestamp": t.isoformat(),
                    "account_id": account_id,
                    "user_id": user_id,
                    "video_id": random.choice(watched_videos),
                    "session_id": session_id,
                    "event_name": "watch_time",
                    "value": value,
                    "device": device,
                    "device_os": device_os,
                    "app_version": app_version,
                    "network_type": network_type,
                    "ip": ip,
                    "country": country,
                })

                if random.random() < 0.12:
                    rows.append({
                        "timestamp": (t + timedelta(seconds=1)).isoformat(),
                        "account_id": account_id,
                        "user_id": user_id,
                        "video_id": random.choice(watched_videos),
                        "session_id": session_id,
                        "event_name": random.choice(["like", "heart"]),
                        "value": None,
                        "device": device,
                        "device_os": device_os,
                        "app_version": app_version,
                        "network_type": network_type,
                        "ip": ip,
                        "country": country,
                    })

            end_time = session_time + timedelta(seconds=session_watch_seconds + random.randint(0, 60))
            rows.append({
                "timestamp": end_time.isoformat(),
                "account_id": account_id,
                "user_id": user_id,
                "video_id": None,
                "session_id": session_id,
                "event_name": "session_end",
                "value": None,
                "device": device,
                "device_os": device_os,
                "app_version": app_version,
                "network_type": network_type,
                "ip": ip,
                "country": country,
            })

            session_time = end_time + timedelta(minutes=random.randint(10, 600))

    return rows


def write_outputs(cfg: GeneratorConfig) -> None:
    random.seed(cfg.seed)
    np.random.seed(cfg.seed)
    ensure_out(cfg.out_dir)

    users_df = generate_users(cfg)
    videos_df = generate_videos(cfg)
    devices_df = generate_devices()
    events = generate_events(cfg, users_df, videos_df)

    users_df.to_csv(cfg.out_dir / "users.csv", index=False)
    videos_df.to_csv(cfg.out_dir / "videos.csv", index=False)
    devices_df.to_csv(cfg.out_dir / "devices.csv", index=False)

    events_path = cfg.out_dir / "events.jsonl"
    with open(events_path, "w", encoding="utf-8") as f:
        for row in events:
            f.write(json.dumps(row) + "\n")

    print(f"Wrote: {events_path}")
    print(f"Wrote: {cfg.out_dir / 'users.csv'}")
    print(f"Wrote: {cfg.out_dir / 'videos.csv'}")
    print(f"Wrote: {cfg.out_dir / 'devices.csv'}")


def parse_args() -> GeneratorConfig:
    parser = argparse.ArgumentParser(description="Generate StreamPro synthetic dataset")
    parser.add_argument(
        "--out-dir",
        type=str,
        default=str(Path(__file__).resolve().parents[1] / "data"),
        help="Output directory for data files",
    )
    parser.add_argument("--days", type=int, default=7, help="Number of days to span events over")
    parser.add_argument("--users", type=int, default=300, help="Number of users to generate")
    parser.add_argument("--videos", type=int, default=80, help="Number of videos to generate")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    args = parser.parse_args()
    return GeneratorConfig(out_dir=Path(args.out_dir), days=args.days, users=args.users, videos=args.videos, seed=args.seed)


def main() -> None:
    cfg = parse_args()
    write_outputs(cfg)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
StreamPro synthetic data generator.

Outputs into a target data directory:
  - users.csv
  - videos.csv
  - devices.csv
  - events.jsonl (NDJSON)

Notes:
  - All comments and strings are in English per project conventions.
  - Designed for local development; no external services required.
"""
from __future__ import annotations

import argparse
import json
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd


@dataclass
class GeneratorConfig:
    out_dir: Path
    days: int = 7
    users: int = 300
    videos: int = 80
    seed: int = 42


def ensure_out(out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)


def generate_users(cfg: GeneratorConfig) -> pd.DataFrame:
    today = datetime.utcnow().date()
    users: List[List[str]] = []
    for i in range(cfg.users):
        user_id = f"u_{i:05d}"
        signup_date = today - timedelta(days=random.randint(1, cfg.days + 7))
        subscription_tier = random.choice(["free", "basic", "premium"])
        age_group = random.choice(["18-24", "25-34", "35-44", "45-54", "55+"])
        gender = random.choice(["female", "male", "other", "prefer_not_to_say"])
        users.append([user_id, str(signup_date), subscription_tier, age_group, gender])
    return pd.DataFrame(
        users, columns=["user_id", "signup_date", "subscription_tier", "age_group", "gender"]
    )


def generate_videos(cfg: GeneratorConfig) -> pd.DataFrame:
    genres = [
        "drama",
        "comedy",
        "documentary",
        "action",
        "horror",
        "sci-fi",
        "fantasy",
        "romance",
    ]
    videos: List[List[object]] = []
    for i in range(cfg.videos):
        video_id = f"v_{i:05d}"
        title = f"Video {i}"
        genre = random.choice(genres)
        duration_seconds = random.randint(30, 3600)
        patent_id = f"pat_{random.randint(1000, 9999)}"
        videos.append([video_id, title, genre, duration_seconds, patent_id])
    return pd.DataFrame(
        videos, columns=["video_id", "title", "genre", "duration_seconds", "patent_id"]
    )


def generate_devices() -> pd.DataFrame:
    rows: List[List[str]] = []
    device_types = ["mobile", "tablet", "desktop"]
    device_models = ["A1", "A2", "B1", "B2", "C1"]
    os_versions = ["iOS 16", "Android 13", "Windows 11", "macOS 14"]
    for d in device_types:
        for m in device_models:
            for o in os_versions:
                rows.append([d, m, o])
    return pd.DataFrame(rows, columns=["device", "device_model", "os_version"])


def generate_events(cfg: GeneratorConfig, users_df: pd.DataFrame, videos_df: pd.DataFrame) -> List[dict]:
    accounts = ["acct_1", "acct_2", "acct_3"]
    device_types = ["mobile", "tablet", "desktop"]
    device_os_options = ["iOS", "Android", "Windows", "macOS"]
    app_versions = ["1.0.0", "1.1.0", "1.2.0", "2.0.0", "2.1.0"]
    network_types = ["wifi", "4G", "5G"]
    countries = ["US", "CA", "GB", "DE", "FR", "BR", "IN", "AU", "JP"]

    base_dt = datetime.utcnow() - timedelta(days=cfg.days)
    rows: List[dict] = []

    user_ids = users_df["user_id"].tolist()
    video_ids = videos_df["video_id"].tolist()

    for user_id in user_ids:
        # draw sessions per user across the window, at least 1
        num_sessions = np.random.poisson(lam=1.8) + 1
        session_time = base_dt + timedelta(hours=random.randint(0, cfg.days * 24))
        first_login_emitted = False

        for s in range(num_sessions):
            account_id = random.choice(accounts)
            session_id = f"s_{user_id}_{s:04d}"
            device = random.choice(device_types)
            device_os = random.choice(device_os_options)
            app_version = random.choice(app_versions)
            network_type = random.choice(network_types)
            ip = f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"
            country = random.choice(countries)

            if not first_login_emitted:
                rows.append({
                    "timestamp": (session_time - timedelta(minutes=5)).isoformat(),
                    "account_id": account_id,
                    "user_id": user_id,
                    "video_id": None,
                    "session_id": session_id,
                    "event_name": "first_login",
                    "value": None,
                    "device": device,
                    "device_os": device_os,
                    "app_version": app_version,
                    "network_type": network_type,
                    "ip": ip,
                    "country": country,
                })
                first_login_emitted = True

            rows.append({
                "timestamp": session_time.isoformat(),
                "account_id": account_id,
                "user_id": user_id,
                "video_id": None,
                "session_id": session_id,
                "event_name": "session_start",
                "value": None,
                "device": device,
                "device_os": device_os,
                "app_version": app_version,
                "network_type": network_type,
                "ip": ip,
                "country": country,
            })

            # simulate watch_time chunks
            session_watch_seconds = 0
            num_chunks = random.randint(1, 10)
            t = session_time
            watched_videos = random.sample(video_ids, k=min(len(video_ids), random.randint(1, 4)))
            for _ in range(num_chunks):
                t += timedelta(seconds=random.randint(1, 50))
                value = int(np.random.exponential(scale=12)) + 1  # skewed distribution
                session_watch_seconds += value
                rows.append({
                    "timestamp": t.isoformat(),
                    "account_id": account_id,
                    "user_id": user_id,
                    "video_id": random.choice(watched_videos),
                    "session_id": session_id,
                    "event_name": "watch_time",
                    "value": value,
                    "device": device,
                    "device_os": device_os,
                    "app_version": app_version,
                    "network_type": network_type,
                    "ip": ip,
                    "country": country,
                })

                if random.random() < 0.12:
                    rows.append({
                        "timestamp": (t + timedelta(seconds=1)).isoformat(),
                        "account_id": account_id,
                        "user_id": user_id,
                        "video_id": random.choice(watched_videos),
                        "session_id": session_id,
                        "event_name": random.choice(["like", "heart"]),
                        "value": None,
                        "device": device,
                        "device_os": device_os,
                        "app_version": app_version,
                        "network_type": network_type,
                        "ip": ip,
                        "country": country,
                    })

            end_time = session_time + timedelta(seconds=session_watch_seconds + random.randint(0, 60))
            rows.append({
                "timestamp": end_time.isoformat(),
                "account_id": account_id,
                "user_id": user_id,
                "video_id": None,
                "session_id": session_id,
                "event_name": "session_end",
                "value": None,
                "device": device,
                "device_os": device_os,
                "app_version": app_version,
                "network_type": network_type,
                "ip": ip,
                "country": country,
            })

            session_time = end_time + timedelta(minutes=random.randint(10, 600))

    return rows


def write_outputs(cfg: GeneratorConfig) -> None:
    random.seed(cfg.seed)
    np.random.seed(cfg.seed)
    ensure_out(cfg.out_dir)

    users_df = generate_users(cfg)
    videos_df = generate_videos(cfg)
    devices_df = generate_devices()
    events = generate_events(cfg, users_df, videos_df)

    users_df.to_csv(cfg.out_dir / "users.csv", index=False)
    videos_df.to_csv(cfg.out_dir / "videos.csv", index=False)
    devices_df.to_csv(cfg.out_dir / "devices.csv", index=False)

    events_path = cfg.out_dir / "events.jsonl"
    with open(events_path, "w", encoding="utf-8") as f:
        for row in events:
            f.write(json.dumps(row) + "\n")

    print(f"Wrote: {events_path}")
    print(f"Wrote: {cfg.out_dir / 'users.csv'}")
    print(f"Wrote: {cfg.out_dir / 'videos.csv'}")
    print(f"Wrote: {cfg.out_dir / 'devices.csv'}")


def parse_args() -> GeneratorConfig:
    parser = argparse.ArgumentParser(description="Generate StreamPro synthetic dataset")
    parser.add_argument(
        "--out-dir",
        type=str,
        default=str(Path(__file__).resolve().parents[1] / "data"),
        help="Output directory for data files",
    )
    parser.add_argument("--days", type=int, default=7, help="Number of days to span events over")
    parser.add_argument("--users", type=int, default=300, help="Number of users to generate")
    parser.add_argument("--videos", type=int, default=80, help="Number of videos to generate")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    args = parser.parse_args()
    return GeneratorConfig(out_dir=Path(args.out_dir), days=args.days, users=args.users, videos=args.videos, seed=args.seed)


def main() -> None:
    cfg = parse_args()
    write_outputs(cfg)


if __name__ == "__main__":
    main()


