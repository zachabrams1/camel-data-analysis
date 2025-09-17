#!/usr/bin/env python3
"""
Event Analytics Script (updated)

Loads attendance.csv, events.csv, people.csv (and optionally invite_tokens.csv)
Recomputes first event per person based on earliest actual check-in, then produces:
1) Retention by event — now a grouped bar chart with two bars per event: total attendees vs attendees who returned later
2) Post-party retention for Launch, Sababa Nights (DX party), BSMNT, Fall 2025 BNL Party
3) New members by event — now includes two bars per event: new members vs total unique attendees
4) New members by event type — now includes adjacent return buckets: exactly 1 return, exactly 2 returns, and 3+ returns
5) Approval friction: not-approved counts and "lost" rate (never returned later)
6) Attendance counts for the big parties
7) RSVP to Attendance histogram showing conversion from RSVPs to actual attendances

Outputs:
- CSVs with computed tables (some new columns/files added)
- PNG charts in the output folder
"""

import argparse
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


def load_data(attendance_path, events_path, people_path, invite_tokens_path=None):
    attendance = pd.read_csv(attendance_path)
    events = pd.read_csv(events_path)
    people = pd.read_csv(people_path)
    invite_tokens = None
    if invite_tokens_path and Path(invite_tokens_path).exists():
        invite_tokens = pd.read_csv(invite_tokens_path)

    # Parse datetimes
    if "rsvp_datetime" in attendance.columns:
        attendance["rsvp_datetime"] = pd.to_datetime(attendance["rsvp_datetime"], errors="coerce")
    if "start_datetime" in events.columns:
        events["start_datetime"] = pd.to_datetime(events["start_datetime"], errors="coerce")

    return attendance, events, people, invite_tokens


def prep_master(attendance, events, people):
    # Normalize common fields
    if "checked_in" in attendance.columns:
        attendance["checked_in"] = attendance["checked_in"].astype(bool)
    if "rsvp" in attendance.columns:
        attendance["rsvp"] = attendance["rsvp"].astype(bool)
    if "approved" in attendance.columns:
        # could be T/F or 0/1
        attendance["approved"] = attendance["approved"].astype(int)

    att = attendance.merge(events, left_on="event_id", right_on="id", suffixes=("", "_event"))
    att = att.merge(people, left_on="person_id", right_on="id", suffixes=("", "_person"))
    return att, events


def recompute_first_events(att):
    # First attended = earliest start_datetime where checked_in == True
    attended = att[att["checked_in"] == True].copy()
    if attended.empty:
        raise ValueError("No rows with checked_in == True; cannot compute first events.")
    first_idx = attended.groupby("person_id")["start_datetime"].idxmin()
    first_events = attended.loc[first_idx, ["person_id", "event_id", "start_datetime"]].rename(
        columns={"event_id": "first_event_id", "start_datetime": "first_event_time"}
    )
    # Attach to master
    att = att.merge(first_events, on="person_id", how="left")
    return att, first_events, attended


def retention_by_event(events, attended):
    rows = []
    for _, row in events.sort_values("start_datetime").iterrows():
        eid = row["id"]
        etime = row["start_datetime"]
        attendees = attended[attended["event_id"] == eid]["person_id"].unique()
        n_att = len(attendees)
        if n_att == 0:
            rows.append({
                "event_id": eid,
                "event_name": row.get("event_name", eid),
                "category": row.get("category"),
                "start_datetime": etime,
                "attendees": 0,
                "returned_later": 0,
                "retention_rate": np.nan,
            })
            continue
        later = attended[(attended["person_id"].isin(attendees)) & (attended["start_datetime"] > etime)]
        n_ret = later["person_id"].nunique()
        rows.append({
            "event_id": eid,
            "event_name": row.get("event_name", eid),
            "category": row.get("category"),
            "start_datetime": etime,
            "attendees": n_att,
            "returned_later": n_ret,
            "retention_rate": n_ret / n_att,
        })
    # Sort chronologically for plotting legibility
    return pd.DataFrame(rows).sort_values(["start_datetime"]).reset_index(drop=True)


def post_party_retention(events, first_events, attended):
    # Identify the four parties (robust matching)
    name_lower = events["event_name"].str.lower().fillna("")
    parties_mask = (
        (name_lower == "launch")
        | (name_lower.str.contains(r"\bsababa nights\b"))
        | (name_lower.str.contains(r"\bbsmnt\b"))
        | (name_lower.str.contains(r"fall\s*2025.*party"))
    )
    parties = events[parties_mask].copy()
    # fallback alias if dataset uses slightly different label for DX
    if parties.empty and name_lower.str.contains("sababa").any():
        parties = events[name_lower.str.contains("sababa", na=False)].copy()

    rows = []
    for _, row in parties.sort_values("start_datetime").iterrows():
        pid = row["id"]
        ptime = row["start_datetime"]
        first_timers = first_events[first_events["first_event_id"] == pid]["person_id"].unique()
        n_first = len(first_timers)
        if n_first == 0:
            rows.append({
                "party_id": pid,
                "party_name": row.get("event_name", pid),
                "party_time": ptime,
                "first_timers": 0,
                "later_returned": 0,
                "post_party_retention": np.nan,
            })
            continue
        later = attended[(attended["person_id"].isin(first_timers)) & (attended["start_datetime"] > ptime)]
        n_returned = later["person_id"].nunique()
        rows.append({
            "party_id": pid,
            "party_name": row.get("event_name", pid),
            "party_time": ptime,
            "first_timers": n_first,
            "later_returned": n_returned,
            "post_party_retention": n_returned / n_first,
        })
    return pd.DataFrame(rows).sort_values("party_time").reset_index(drop=True)


def new_members_tables(first_events, events, attended):
    """Return three tables:
    - by_event: per-event new members and total unique attendees
    - by_type: per-category new members
    - by_type_returns: per-category counts of first-timers who returned exactly 1x, exactly 2x, and 3+ times
    """
    first_with_meta = first_events.merge(
        events[["id", "event_name", "category", "start_datetime"]],
        left_on="first_event_id",
        right_on="id",
        how="left",
    )

    # New members by event (first-time attendees)
    by_event = (
        first_with_meta.groupby(["first_event_id", "event_name", "category", "start_datetime"])["person_id"]
        .nunique()
        .reset_index(name="new_members")
    )
    # Total unique attendees by event
    total_att = (
        attended.groupby("event_id")["person_id"].nunique().reset_index(name="total_attendees")
    )
    by_event = by_event.merge(
        total_att, left_on="first_event_id", right_on="event_id", how="left"
    ).drop(columns=["event_id"]).sort_values("new_members", ascending=False)

    # New members by type (category)
    by_type = (
        first_with_meta.groupby("category")["person_id"].nunique().reset_index(name="new_members")
    ).sort_values("new_members", ascending=False)

    # --- Return buckets per category (exactly 1, exactly 2, and 3+ future attendances) ---
    # For each person, count future attendances strictly after their first_event_time
    tmp = attended.merge(
        first_events[["person_id", "first_event_time"]], on="person_id", how="left"
    )
    tmp_future = tmp[tmp["start_datetime"] > tmp["first_event_time"]].copy()
    returns_per_person = (
        tmp_future.groupby("person_id").size().reset_index(name="n_returns")
    )
    # Ensure everyone appears (including 0 returns)
    returns_per_person = first_events[["person_id"]].merge(
        returns_per_person, on="person_id", how="left"
    ).fillna({"n_returns": 0})

    # Attach category of first event
    returns_with_cat = returns_per_person.merge(
        first_with_meta[["person_id", "category"]], on="person_id", how="left"
    )

    def bucket_counts(g):
        n1 = (g["n_returns"] == 1).sum()
        n2 = (g["n_returns"] == 2).sum()
        n3p = (g["n_returns"] >= 3).sum()
        nm = g.shape[0]
        return pd.Series({
            "new_members": nm,
            "returned_1": n1,
            "returned_2": n2,
            "returned_3plus": n3p,
        })

    by_type_returns = (
        returns_with_cat.groupby("category").apply(bucket_counts).reset_index()
    ).sort_values("new_members", ascending=False)

    return by_event, by_type, by_type_returns


def approval_loss(att, attended):
    # People who RSVP'd but not approved for that event
    if "rsvp" not in att.columns or "approved" not in att.columns:
        return pd.DataFrame()
    na = (
        att[(att["rsvp"] == True) & (att["approved"] == 0)]
        .drop_duplicates(subset=["person_id", "event_id"]).copy()
    )

    def lost_fn(group):
        etime = group["start_datetime"].iloc[0]
        persons = group["person_id"].unique()
        later = attended[(attended["person_id"].isin(persons)) & (attended["start_datetime"] > etime)]
        returned = set(later["person_id"].unique())
        lost = [p for p in persons if p not in returned]
        return pd.Series(
            {
                "not_approved_count": len(persons),
                "lost_count": len(lost),
                "lost_rate": (len(lost) / len(persons)) if len(persons) else np.nan,
            }
        )

    return (
        na.groupby(["event_id", "event_name", "start_datetime"]).apply(lost_fn).reset_index()
    ).sort_values("lost_rate", ascending=False)


def party_attendance_counts(events, attended):
    # Attendance counts for the four parties
    name_lower = events["event_name"].str.lower().fillna("")
    parties_mask = (
        (name_lower == "launch")
        | (name_lower.str.contains(r"\bsababa nights\b"))
        | (name_lower.str.contains(r"\bbsmnt\b"))
        | (name_lower.str.contains(r"fall\s*2025.*party"))
    )
    parties = events[parties_mask].copy()
    counts = (
        attended[attended["event_id"].isin(parties["id"])].groupby("event_id")["person_id"].nunique().reset_index(name="attendee_count")
        .merge(events[["id", "event_name", "start_datetime"]], left_on="event_id", right_on="id")
        .sort_values("start_datetime")
    )
    return counts


# -------------------- Plotting --------------------

def _grouped_bar(ax, labels, series_list, series_labels, width=0.38, rotate=45):
    x = np.arange(len(labels))
    n = len(series_list)
    offsets = np.linspace(-width * (n - 1) / 2, width * (n - 1) / 2, n)
    for s, off, lab in zip(series_list, offsets, series_labels):
        ax.bar(x + off, s, width, label=lab)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=rotate, ha="right")
    ax.legend()
    ax.margins(x=0.01)


def plot_retention_by_event(df, outdir):
    """Grouped bars: total attendees vs attendees who returned later."""
    dfp = df.sort_values("start_datetime")
    labels = dfp["event_name"].tolist()
    attendees = dfp["attendees"].fillna(0).astype(int).tolist()
    returned = dfp["returned_later"].fillna(0).astype(int).tolist()

    fig, ax = plt.subplots(figsize=(12, 6))
    _grouped_bar(ax, labels, [attendees, returned], ["Attendees", "Returned later"], width=0.42, rotate=75)
    ax.set_ylabel("People (count)")
    ax.set_title("Attendees vs Returned Later — by Event")
    fig.tight_layout()
    fp = outdir / "retention_by_event.png"
    fig.savefig(fp, dpi=150)
    plt.close(fig)
    return fp


def plot_post_party(df, outdir):
    plt.figure(figsize=(10, 6))
    plt.bar(df["party_name"], (df["post_party_retention"] * 100).fillna(0))
    plt.ylabel("Post-Party Retention (%)")
    plt.title("Post-Party Retention (First Event = That Party)\nLaunch, Sababa Nights, BS MNT, Fall 2025")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    fp = outdir / "post_party_retention.png"
    plt.savefig(fp, dpi=150)
    plt.close()
    return fp


def plot_new_members_by_event(by_event, outdir, top=16):
    """Grouped bars: new members vs total unique attendees for top-N events by new members."""
    top_events = by_event.sort_values("new_members", ascending=False).head(top)
    labels = top_events["event_name"].tolist()
    newm = top_events["new_members"].tolist()
    total = top_events["total_attendees"].fillna(0).astype(int).tolist()

    fig, ax = plt.subplots(figsize=(12, 6))
    _grouped_bar(ax, labels, [total, newm], ["Total attendees", "New members (first-timers)"], width=0.42, rotate=45)
    ax.set_ylabel("People (count)")
    ax.set_title(f"Top {len(labels)} Events — New Members vs Total Attendees")
    fig.tight_layout()
    fp = outdir / "new_members_top_events.png"
    fig.savefig(fp, dpi=150)
    plt.close(fig)
    return fp


def plot_post_party_three_bars(party_conv_df, party_counts, outdir):
    """
    Produces a grouped bar chart per party:
      - total attendees
      - first-timers
      - first-timers who returned later
    """
    # Merge totals in
    df = party_conv_df.merge(
        party_counts[["event_id", "attendee_count"]],
        left_on="party_id",
        right_on="event_id",
        how="left",
    ).drop(columns=["event_id"])

    # Ensure stable chronological order
    df = df.sort_values("party_time").copy()

    # Rename for clarity
    df = df.rename(
        columns={
            "attendee_count": "attendees",
            "first_timers": "first_timers",
            "later_returned": "first_then_returned",
        }
    )

    # Fill any missing counts with 0 to avoid NaNs in bars
    for c in ["attendees", "first_timers", "first_then_returned"]:
        df[c] = df[c].fillna(0).astype(int)

    labels = df["party_name"].tolist()
    x = np.arange(len(labels))
    width = 0.28

    plt.figure(figsize=(12, 6))
    plt.bar(x - width, df["attendees"], width, label="Attendees")
    plt.bar(x, df["first_timers"], width, label="First-timers")
    plt.bar(x + width, df["first_then_returned"], width, label="First-timers who returned")

    plt.xticks(x, labels, rotation=45, ha="right")
    plt.ylabel("People (count)")
    plt.title("Post-Party Funnel: Attendees vs First-Timers vs First-Timers Who Returned")
    plt.legend()
    plt.tight_layout()

    fp = outdir / "post_party_three_bars.png"
    plt.savefig(fp, dpi=150)
    plt.close()
    return fp


def plot_new_members_by_type(by_type_returns, outdir):
    """Grouped bars per category: New members, Returned 1x, Returned 2x, Returned 3+x."""
    dfp = by_type_returns.copy()
    labels = dfp["category"].fillna("Unknown").tolist()
    nm = dfp["new_members"].astype(int).tolist()
    r1 = dfp["returned_1"].astype(int).tolist()
    r2 = dfp["returned_2"].astype(int).tolist()
    r3p = dfp["returned_3plus"].astype(int).tolist()

    fig, ax = plt.subplots(figsize=(12, 6))
    _grouped_bar(
        ax,
        labels,
        [nm, r1, r2, r3p],
        ["New members", "Returned 1x", "Returned 2x", "Returned 3+"],
        width=0.2,
        rotate=45,
    )
    ax.set_ylabel("People (count)")
    ax.set_title("New Members by Event Type with Return Buckets")
    fig.tight_layout()

    fp = outdir / "new_members_by_type.png"
    fig.savefig(fp, dpi=150)
    plt.close(fig)
    return fp


# -------------------- RSVP to Attendance Histogram --------------------

def compute_attendance_per_person(att):
    """Compute number of total attendances per person who RSVPed (integer >= 0)."""
    # Start with people who RSVPed
    rsvped = att[att["rsvp"] == True].copy()
    
    # Count attendances (checked_in = True) per person
    attended_counts = rsvped[rsvped["checked_in"] == True].groupby("person_id").size().reset_index(name="n_attendances")
    
    # Get all people who RSVPed (to include 0-attendance people)
    all_rsvped = rsvped[["person_id"]].drop_duplicates()
    
    # Merge to include people with 0 attendances
    attendance_counts = all_rsvped.merge(attended_counts, on="person_id", how="left").fillna({"n_attendances": 0})
    attendance_counts["n_attendances"] = attendance_counts["n_attendances"].astype(int)
    
    return attendance_counts


def plot_attendance_histogram(attendance_df, outdir):
    """Plot histogram (discrete) of attendance counts across all people who RSVPed, with bar labels."""
    max_att = int(attendance_df["n_attendances"].max()) if not attendance_df.empty else 0
    xs = np.arange(0, max_att + 1)
    counts = attendance_df["n_attendances"].value_counts().sort_index()
    ys = [int(counts.get(i, 0)) for i in xs]

    total_rsvps = sum(ys)
    total_attendees = sum(ys[1:])

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(xs, ys)
    ax.set_xlabel("Number of events attended")
    ax.set_ylabel("People (count)")
    ax.set_title(f"RSVP to Attendance Conversion — N={total_rsvps:,} total RSVPs - N={total_attendees:,} total Attending Members")
    ax.set_xticks(xs)

    # Label each bar with its count
    try:
        # Matplotlib >= 3.4
        ax.bar_label(bars, padding=3, fmt='%.0f')
    except Exception:
        # Fallback for older Matplotlib
        for rect in bars:
            height = rect.get_height()
            ax.text(
                rect.get_x() + rect.get_width() / 2.0,
                height,
                f"{int(height)}",
                ha="center",
                va="bottom",
                fontsize=9,
            )

    # Add note about 0 column
    ax.text(0.02, 0.95, 
            "0 = RSVPed but never attended\n1+ = Number of events attended", 
            transform=ax.transAxes, 
            verticalalignment='top',
            bbox=dict(boxstyle='round', facecolor='lightgray', alpha=0.8))

    fig.tight_layout()

    fp = outdir / "rsvp_attendance_histogram.png"
    fig.savefig(fp, dpi=150)
    plt.close(fig)
    return fp

# -------------------- Main --------------------

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--attendance", default="final/attendance.csv")
    p.add_argument("--events", default="final/events.csv")
    p.add_argument("--people", default="final/people.csv")
    p.add_argument("--invite_tokens", default=None)
    p.add_argument("--outdir", default="analysis_outputs")
    args = p.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    attendance, events, people, invite_tokens = load_data(
        args.attendance, args.events, args.people, args.invite_tokens
    )
    att, events = prep_master(attendance, events, people)
    att, first_events, attended = recompute_first_events(att)

    # 1) Retention by event (now two bars per event)
    retention_df = retention_by_event(events, attended)
    plot_retention_by_event(retention_df, outdir)

    # 2) Post-party retention
    party_conv_df = post_party_retention(events, first_events, attended)
    plot_post_party(party_conv_df, outdir)

    # 3) New members
    new_by_event, new_by_type, by_type_returns = new_members_tables(first_events, events, attended)

    # 3b) RSVP to Attendance histogram
    attendance_counts_df = compute_attendance_per_person(att)
    hist_counts = attendance_counts_df["n_attendances"].value_counts().sort_index().reset_index()
    hist_counts.columns = ["n_attendances", "people_count"]
    
    plot_attendance_histogram(attendance_counts_df, outdir)
    plot_new_members_by_event(new_by_event, outdir)
    plot_new_members_by_type(by_type_returns, outdir)

    # 5) Attendance counts for big parties
    party_counts = party_attendance_counts(events, attended)

    # Party funnel 3-bar chart
    plot_post_party_three_bars(party_conv_df, party_counts, outdir)

    # 6) Approval loss analysis
    approval_df = approval_loss(att, attended)
    

    print("Done! Results written to:", outdir.resolve())
    print(f"Generated RSVP to Attendance histogram with {len(attendance_counts_df)} total RSVPs")
    print(f"No-shows (0 attendances): {(attendance_counts_df['n_attendances'] == 0).sum()}")


if __name__ == "__main__":
    main()