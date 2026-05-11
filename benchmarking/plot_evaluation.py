#!/usr/bin/env python3
import argparse
import json
import os
import tempfile
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", str(Path(tempfile.gettempdir()) / "cs598-matplotlib"))

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate evaluation plots for distributed RAG benchmarks."
    )
    parser.add_argument(
        "--attempt-metrics",
        default="benchmarking/attempt_metrics.jsonl",
        help="JSONL from benchmarking/summarize_events.py.",
    )
    parser.add_argument(
        "--embedding-comparison",
        default="benchmarking/document_embedding_comparison.jsonl",
        help="JSONL from rag/batch_compare_document_embeddings.py.",
    )
    parser.add_argument(
        "--output-dir",
        default="benchmarking/plots",
        help="Directory for generated PNGs and plot_index.json.",
    )
    parser.add_argument(
        "--modeled-average-hops",
        type=float,
        default=None,
        help=(
            "Average number of PageIndex-probing hops for modeled no-wait query latency. "
            "Defaults to the mean nodes_contacted value from attempt metrics."
        ),
    )
    parser.add_argument(
        "--modeled-summary-seconds",
        type=float,
        default=5.2,
        help="Average final summary LLM latency for modeled no-wait query latency.",
    )
    parser.add_argument("--dpi", type=int, default=180)
    return parser.parse_args()


def read_jsonl(path: Path):
    if not path.exists():
        return []
    rows = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def to_dataframe(path: Path):
    return pd.DataFrame(read_jsonl(path))


def bool_series(series):
    return series.fillna(False).astype(bool)


def numeric(df, column):
    if column not in df:
        return pd.Series(dtype=float)
    return pd.to_numeric(df[column], errors="coerce")


def save_plot(fig, output_dir: Path, name: str, description: str, index: list[dict], dpi: int):
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{name}.png"
    fig.tight_layout()
    fig.savefig(path, dpi=dpi)
    plt.close(fig)
    index.append(
        {
            "file": str(path),
            "name": name,
            "description": description,
        }
    )


def bar_labels(ax, fmt="{:.2f}", rotation=0):
    for patch in ax.patches:
        height = patch.get_height()
        if np.isnan(height):
            continue
        ax.text(
            patch.get_x() + patch.get_width() / 2,
            height,
            fmt.format(height),
            ha="center",
            va="bottom",
            fontsize=8,
            rotation=rotation,
        )


def zoom_axis_to_values(ax, values, min_padding=0.02):
    values = pd.Series(values).dropna()
    if values.empty:
        return
    lower = float(values.min())
    upper = float(values.max())
    span = upper - lower
    padding = max(span * 0.18, min_padding)
    ax.set_ylim(
        max(0.0, lower - padding),
        min(1.0, upper + padding),
    )


def percentile(series, pct):
    values = pd.Series(series).dropna().sort_values().to_numpy()
    if len(values) == 0:
        return None
    return float(np.percentile(values, pct))


def infer_average_hops(df):
    if "nodes_contacted" in df:
        nodes_contacted = numeric(df, "nodes_contacted")
        nodes_contacted = nodes_contacted[nodes_contacted > 0].dropna()
        if not nodes_contacted.empty:
            return float(nodes_contacted.mean()), "mean(nodes_contacted)"

    if "max_hop" in df:
        hop_counts = numeric(df, "max_hop")
        hop_counts = hop_counts[hop_counts >= 0].dropna() + 1
        if not hop_counts.empty:
            return float(hop_counts.mean()), "mean(max_hop + 1)"

    return 1.0, "fallback"


def plot_retrieval_success_by_topic(df, output_dir, index, dpi):
    required = {"topic", "retrieval_success"}
    if not required.issubset(df.columns):
        return
    grouped = (
        df.assign(retrieval_success=bool_series(df["retrieval_success"]))
        .groupby("topic")["retrieval_success"]
        .mean()
        .sort_values()
    )
    if grouped.empty:
        return
    fig, ax = plt.subplots(figsize=(10, 6))
    grouped.plot(kind="barh", ax=ax, color="#2f6f9f")
    ax.set_title("Retrieval Success Rate by Topic")
    ax.set_xlabel("Success Rate")
    ax.set_ylabel("Topic")
    ax.set_xlim(0, 1)
    save_plot(
        fig,
        output_dir,
        "retrieval_success_by_topic",
        "Fraction of attempts with evidence returned, grouped by benchmark topic.",
        index,
        dpi,
    )


def plot_latency_distribution(df, output_dir, index, dpi):
    if "latency_seconds" not in df:
        return
    latency = numeric(df, "latency_seconds").dropna()
    if latency.empty:
        return
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.hist(latency, bins=25, color="#406c5a", edgecolor="white")
    ax.axvline(latency.mean(), color="#b64242", linestyle="--", label=f"mean={latency.mean():.1f}s")
    ax.axvline(latency.median(), color="#444444", linestyle=":", label=f"median={latency.median():.1f}s")
    ax.set_title("End-to-End Query Latency")
    ax.set_xlabel("Latency (seconds)")
    ax.set_ylabel("Attempts")
    ax.legend()
    save_plot(
        fig,
        output_dir,
        "latency_distribution",
        "Distribution of end-to-end latency from query dispatch to final answer.",
        index,
        dpi,
    )


def plot_latency_by_outcome(df, output_dir, index, dpi):
    if "latency_seconds" not in df or "retrieval_success" not in df:
        return
    data = df.copy()
    data["latency_seconds"] = numeric(data, "latency_seconds")
    data["retrieval_success"] = bool_series(data["retrieval_success"]).map({True: "success", False: "no evidence"})
    data = data.dropna(subset=["latency_seconds"])
    if data.empty:
        return
    groups = [
        data.loc[data["retrieval_success"] == label, "latency_seconds"].to_numpy()
        for label in ["success", "no evidence"]
    ]
    groups = [values for values in groups if len(values)]
    labels = [
        label
        for label in ["success", "no evidence"]
        if len(data.loc[data["retrieval_success"] == label, "latency_seconds"])
    ]
    if not groups:
        return
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.boxplot(groups, tick_labels=labels, showmeans=True)
    ax.set_title("Latency by Retrieval Outcome")
    ax.set_ylabel("Latency (seconds)")
    save_plot(
        fig,
        output_dir,
        "latency_by_retrieval_outcome",
        "End-to-end latency split by whether the route returned evidence.",
        index,
        dpi,
    )


def plot_hop_distribution(df, output_dir, index, dpi):
    if "max_hop" not in df:
        return
    hops = numeric(df, "max_hop").dropna().astype(int)
    if hops.empty:
        return
    counts = hops.value_counts().sort_index()
    fig, ax = plt.subplots(figsize=(8, 5))
    counts.plot(kind="bar", ax=ax, color="#6b5f8f")
    ax.set_title("Hop Count Distribution")
    ax.set_xlabel("Max Hop Observed")
    ax.set_ylabel("Attempts")
    save_plot(
        fig,
        output_dir,
        "hop_count_distribution",
        "How many chain-hop steps were observed per attempt.",
        index,
        dpi,
    )


def plot_success_by_hop(df, output_dir, index, dpi):
    if not {"max_hop", "retrieval_success"}.issubset(df.columns):
        return
    data = df.copy()
    data["max_hop"] = numeric(data, "max_hop")
    data["retrieval_success"] = bool_series(data["retrieval_success"])
    data = data.dropna(subset=["max_hop"])
    if data.empty:
        return
    grouped = data.groupby("max_hop")["retrieval_success"].mean().sort_index()
    fig, ax = plt.subplots(figsize=(8, 5))
    grouped.plot(marker="o", ax=ax, color="#2f6f9f")
    ax.set_title("Retrieval Success by Hop Count")
    ax.set_xlabel("Max Hop Observed")
    ax.set_ylabel("Success Rate")
    ax.set_ylim(0, 1)
    save_plot(
        fig,
        output_dir,
        "success_by_hop_count",
        "Evidence-return success rate grouped by observed hop count.",
        index,
        dpi,
    )


def plot_nodes_contacted(df, output_dir, index, dpi):
    if not {"nodes_contacted", "retrieval_success"}.issubset(df.columns):
        return
    data = df.copy()
    data["nodes_contacted"] = numeric(data, "nodes_contacted")
    data["retrieval_success"] = bool_series(data["retrieval_success"])
    data = data.dropna(subset=["nodes_contacted"])
    if data.empty:
        return
    grouped = data.groupby("nodes_contacted")["retrieval_success"].agg(["mean", "count"])
    grouped = grouped.sort_index()
    positions = np.arange(len(grouped))
    labels = [str(int(value)) for value in grouped.index]
    fig, ax1 = plt.subplots(figsize=(9, 5))
    ax1.bar(positions, grouped["mean"].to_numpy(), color="#2f6f9f")
    ax1.set_title("Success Rate by Nodes Contacted")
    ax1.set_xlabel("Nodes Contacted")
    ax1.set_ylabel("Success Rate")
    ax1.set_ylim(0, 1)
    ax1.set_xticks(positions)
    ax1.set_xticklabels(labels)
    ax2 = ax1.twinx()
    ax2.plot(
        positions,
        grouped["count"].to_numpy(),
        color="#8a4f2a",
        marker="o",
        linewidth=2,
    )
    ax2.set_ylabel("Attempt Count")
    save_plot(
        fig,
        output_dir,
        "success_by_nodes_contacted",
        "Evidence success rate by number of contacted nodes, with attempt counts.",
        index,
        dpi,
    )


def plot_modeled_query_latency(
    df,
    output_dir,
    index,
    dpi,
    modeled_average_hops,
    modeled_summary_seconds,
    modeled_average_hops_source,
):
    values = []
    if "pageindex_latency_seconds_avg" in df:
        values = [
            value
            for value in numeric(df, "pageindex_latency_seconds_avg").dropna().tolist()
            if value > 0
        ]
    if not values and "pageindex_latencies" in df:
        for latencies in df["pageindex_latencies"].dropna():
            if isinstance(latencies, list):
                call_values = [
                    float(item["latency_seconds"])
                    for item in latencies
                    if isinstance(item, dict)
                    and item.get("latency_seconds") is not None
                ]
                if call_values:
                    values.append(sum(call_values) / len(call_values))
    if not values:
        return

    pageindex_series = pd.Series(values)
    modeled = (pageindex_series * modeled_average_hops) + modeled_summary_seconds
    percentiles = {
        "count": int(modeled.count()),
        "mean": float(modeled.mean()),
        "p50": percentile(modeled, 50),
        "p90": percentile(modeled, 90),
        "p95": percentile(modeled, 95),
        "p99": percentile(modeled, 99),
        "modeled_average_hops": modeled_average_hops,
        "modeled_average_hops_source": modeled_average_hops_source,
        "modeled_summary_seconds": modeled_summary_seconds,
    }

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.hist(modeled, bins=25, color="#5a6d9f", edgecolor="white")
    ax.axvline(
        modeled.mean(),
        color="#b64242",
        linestyle="--",
        label=f"mean={modeled.mean():.1f}s",
    )
    ax.axvline(
        modeled.median(),
        color="#444444",
        linestyle=":",
        label=f"median={modeled.median():.1f}s",
    )
    for label, value, color in [
        ("p90", percentiles["p90"], "#8a4f2a"),
        ("p95", percentiles["p95"], "#6b5f8f"),
    ]:
        if value is not None:
            ax.axvline(
                value,
                color=color,
                linestyle="-.",
                linewidth=1.2,
                label=f"{label}={value:.1f}s",
            )
    ax.set_title("Query Latency")
    ax.set_xlabel("Latency (seconds)")
    ax.set_ylabel("Attempts")
    ax.legend()
    output_dir.mkdir(parents=True, exist_ok=True)
    percentile_path = output_dir / "query_latency_percentiles.json"
    percentile_path.write_text(
        json.dumps(percentiles, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    save_plot(
        fig,
        output_dir,
        "pageindex_call_latency_distribution",
        (
            "Modeled no-wait end-to-end query latency from average PageIndex "
            "call latency, average hop count, and final summary latency."
        ),
        index,
        dpi,
    )


def plot_pageindex_calls_vs_latency(df, output_dir, index, dpi):
    if not {"pageindex_calls", "latency_seconds"}.issubset(df.columns):
        return
    data = df.copy()
    data["pageindex_calls"] = numeric(data, "pageindex_calls")
    data["latency_seconds"] = numeric(data, "latency_seconds")
    data = data.dropna(subset=["pageindex_calls", "latency_seconds"])
    if data.empty:
        return
    fig, ax = plt.subplots(figsize=(8, 5))
    colors = bool_series(data.get("retrieval_success", pd.Series(False, index=data.index))).map(
        {True: "#2f6f9f", False: "#b64242"}
    )
    ax.scatter(data["pageindex_calls"], data["latency_seconds"], c=colors, alpha=0.75)
    ax.set_title("PageIndex Calls vs End-to-End Latency")
    ax.set_xlabel("PageIndex Calls")
    ax.set_ylabel("Latency (seconds)")
    save_plot(
        fig,
        output_dir,
        "pageindex_calls_vs_latency",
        "Relationship between number of PageIndex probes and final-answer latency.",
        index,
        dpi,
    )


def plot_source_reached(df, output_dir, index, dpi):
    if not {"source_reached", "retrieval_success"}.issubset(df.columns):
        return
    data = df.copy()
    data["source_reached"] = bool_series(data["source_reached"])
    data["retrieval_success"] = bool_series(data["retrieval_success"])
    grouped = data.groupby("source_reached")["retrieval_success"].agg(["mean", "count"])
    if grouped.empty:
        return
    fig, ax = plt.subplots(figsize=(7, 5))
    grouped["mean"].rename({False: "source not reached", True: "source reached"}).plot(
        kind="bar",
        ax=ax,
        color=["#8a4f2a", "#2f6f9f"][: len(grouped)],
    )
    ax.set_title("Success When Gold Source Was Reached")
    ax.set_xlabel("")
    ax.set_ylabel("Success Rate")
    ax.set_ylim(0, 1)
    save_plot(
        fig,
        output_dir,
        "success_by_source_reached",
        "Evidence success rate conditioned on whether the known source worker was contacted.",
        index,
        dpi,
    )


def plot_embedding_similarity_boxplot(df, output_dir, index, dpi):
    columns = [
        ("query_to_whole_doc_cosine", "whole doc"),
        ("query_to_pageindex_avg_cosine", "PageIndex all nodes"),
        ("query_to_pageindex_top_level_root_avg_cosine", "top-level root"),
        ("query_to_pageindex_depth_weighted_avg_cosine", "depth weighted"),
    ]
    present = [(column, label) for column, label in columns if column in df]
    if len(present) < 2:
        return
    data = [numeric(df, column).dropna().to_numpy() for column, _ in present]
    labels = [label for _, label in present]
    data = [values for values in data if len(values)]
    if not data:
        return
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.boxplot(data, tick_labels=labels, showmeans=True)
    ax.set_title("Query-to-Document Embedding Similarity")
    ax.set_ylabel("Cosine Similarity")
    ax.tick_params(axis="x", rotation=15)
    save_plot(
        fig,
        output_dir,
        "embedding_similarity_boxplot",
        "Cosine similarity distribution for each document embedding strategy.",
        index,
        dpi,
    )


def plot_embedding_delta_boxplot(df, output_dir, index, dpi):
    columns = [
        ("pageindex_minus_whole", "all nodes - whole"),
        ("pageindex_top_level_root_minus_whole", "top-level - whole"),
        ("pageindex_depth_weighted_minus_whole", "depth weighted - whole"),
        ("pageindex_depth_weighted_minus_all_nodes", "depth weighted - all nodes"),
    ]
    present = [(column, label) for column, label in columns if column in df]
    if not present:
        return
    data = [numeric(df, column).dropna().to_numpy() for column, _ in present]
    labels = [label for _, label in present]
    data = [values for values in data if len(values)]
    if not data:
        return
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.axhline(0, color="#444444", linewidth=1)
    ax.boxplot(data, tick_labels=labels, showmeans=True)
    ax.set_title("Embedding Similarity Deltas")
    ax.set_ylabel("Cosine Difference")
    ax.tick_params(axis="x", rotation=15)
    save_plot(
        fig,
        output_dir,
        "embedding_delta_boxplot",
        "Cosine deltas between PageIndex-derived embeddings and baselines.",
        index,
        dpi,
    )


def plot_embedding_best_counts(df, output_dir, index, dpi):
    if "best_embedding" not in df:
        return
    counts = df["best_embedding"].dropna().value_counts()
    if counts.empty:
        return
    fig, ax = plt.subplots(figsize=(9, 5))
    counts.plot(kind="bar", ax=ax, color="#6b5f8f")
    ax.set_title("Best Embedding Strategy by Query")
    ax.set_xlabel("Embedding Strategy")
    ax.set_ylabel("Query Count")
    ax.tick_params(axis="x", rotation=20)
    save_plot(
        fig,
        output_dir,
        "embedding_best_strategy_counts",
        "Count of queries where each embedding strategy had the highest cosine similarity.",
        index,
        dpi,
    )


def plot_embedding_by_dataset(df, output_dir, index, dpi):
    if "dataset_source" not in df:
        return
    columns = [
        ("query_to_whole_doc_cosine", "whole doc"),
        ("query_to_pageindex_avg_cosine", "all nodes"),
        ("query_to_pageindex_top_level_root_avg_cosine", "top-level"),
        ("query_to_pageindex_depth_weighted_avg_cosine", "depth weighted"),
    ]
    present = [(column, label) for column, label in columns if column in df]
    if len(present) < 2:
        return
    grouped = df.groupby("dataset_source")[[column for column, _ in present]].mean(numeric_only=True)
    if grouped.empty:
        return
    grouped = grouped.rename(columns={column: label for column, label in present})
    fig, ax = plt.subplots(figsize=(10, 5))
    grouped.plot(kind="bar", ax=ax)
    ax.set_title("Average Similarity by Dataset Source")
    ax.set_xlabel("Dataset")
    ax.set_ylabel("Mean Cosine Similarity")
    zoom_axis_to_values(ax, grouped.to_numpy().ravel(), min_padding=0.015)
    ax.tick_params(axis="x", rotation=0)
    ax.legend(title="Embedding", fontsize=8)
    save_plot(
        fig,
        output_dir,
        "embedding_similarity_by_dataset_source",
        "Mean query-to-document cosine similarity split by RepliQA vs FinanceBench.",
        index,
        dpi,
    )


def plot_pages_vs_similarity(df, output_dir, index, dpi):
    if not {"page_count", "query_to_pageindex_avg_cosine", "query_to_whole_doc_cosine"}.issubset(df.columns):
        return
    data = df.copy()
    data["page_count"] = numeric(data, "page_count")
    data["pageindex_minus_whole"] = numeric(data, "pageindex_minus_whole")
    data = data.dropna(subset=["page_count", "pageindex_minus_whole"])
    if data.empty:
        return
    fig, ax = plt.subplots(figsize=(8, 5))
    colors = data.get("dataset_source", pd.Series("unknown", index=data.index)).astype(str)
    palette = {name: color for name, color in zip(sorted(colors.unique()), ["#2f6f9f", "#8a4f2a", "#6b5f8f", "#406c5a"])}
    ax.scatter(
        data["page_count"],
        data["pageindex_minus_whole"],
        c=colors.map(palette),
        alpha=0.75,
    )
    ax.axhline(0, color="#444444", linewidth=1)
    ax.set_title("Page Count vs PageIndex Advantage")
    ax.set_xlabel("Document Pages")
    ax.set_ylabel("All-node cosine - whole-doc cosine")
    save_plot(
        fig,
        output_dir,
        "page_count_vs_pageindex_advantage",
        "Whether PageIndex averaging helps more on longer documents.",
        index,
        dpi,
    )


def plot_depth_weight_by_depth(df, output_dir, index, dpi):
    if not {"pageindex_max_depth", "pageindex_depth_weighted_minus_all_nodes"}.issubset(df.columns):
        return
    data = df.copy()
    data["pageindex_max_depth"] = numeric(data, "pageindex_max_depth")
    data["pageindex_depth_weighted_minus_all_nodes"] = numeric(data, "pageindex_depth_weighted_minus_all_nodes")
    data = data.dropna(subset=["pageindex_max_depth", "pageindex_depth_weighted_minus_all_nodes"])
    if data.empty:
        return
    grouped = data.groupby("pageindex_max_depth")["pageindex_depth_weighted_minus_all_nodes"].mean().sort_index()
    fig, ax = plt.subplots(figsize=(8, 5))
    grouped.plot(kind="bar", ax=ax, color="#406c5a")
    ax.axhline(0, color="#444444", linewidth=1)
    ax.set_title("Depth Weighting Effect by Tree Depth")
    ax.set_xlabel("Max Tree Depth")
    ax.set_ylabel("Depth-weighted cosine - all-node cosine")
    save_plot(
        fig,
        output_dir,
        "depth_weighting_effect_by_tree_depth",
        "Average change in query similarity from depth weighting, grouped by tree depth.",
        index,
        dpi,
    )


def plot_attempt_metrics(
    attempt_df,
    output_dir,
    index,
    dpi,
    modeled_average_hops,
    modeled_summary_seconds,
    modeled_average_hops_source,
):
    if attempt_df.empty:
        return
    plot_retrieval_success_by_topic(attempt_df, output_dir, index, dpi)
    plot_latency_distribution(attempt_df, output_dir, index, dpi)
    plot_latency_by_outcome(attempt_df, output_dir, index, dpi)
    plot_hop_distribution(attempt_df, output_dir, index, dpi)
    plot_success_by_hop(attempt_df, output_dir, index, dpi)
    plot_nodes_contacted(attempt_df, output_dir, index, dpi)
    plot_modeled_query_latency(
        attempt_df,
        output_dir,
        index,
        dpi,
        modeled_average_hops,
        modeled_summary_seconds,
        modeled_average_hops_source,
    )
    plot_pageindex_calls_vs_latency(attempt_df, output_dir, index, dpi)
    plot_source_reached(attempt_df, output_dir, index, dpi)


def plot_embedding_metrics(embedding_df, output_dir, index, dpi):
    if embedding_df.empty:
        return
    plot_embedding_similarity_boxplot(embedding_df, output_dir, index, dpi)
    plot_embedding_delta_boxplot(embedding_df, output_dir, index, dpi)
    plot_embedding_best_counts(embedding_df, output_dir, index, dpi)
    plot_embedding_by_dataset(embedding_df, output_dir, index, dpi)
    plot_pages_vs_similarity(embedding_df, output_dir, index, dpi)
    plot_depth_weight_by_depth(embedding_df, output_dir, index, dpi)


def main():
    args = parse_args()
    output_dir = Path(args.output_dir)
    plot_index = []

    attempt_df = to_dataframe(Path(args.attempt_metrics))
    embedding_df = to_dataframe(Path(args.embedding_comparison))
    inferred_average_hops, average_hops_source = infer_average_hops(attempt_df)
    modeled_average_hops = (
        args.modeled_average_hops
        if args.modeled_average_hops is not None
        else inferred_average_hops
    )
    modeled_average_hops_source = (
        "cli"
        if args.modeled_average_hops is not None
        else average_hops_source
    )

    plot_attempt_metrics(
        attempt_df,
        output_dir,
        plot_index,
        args.dpi,
        modeled_average_hops,
        args.modeled_summary_seconds,
        modeled_average_hops_source,
    )
    plot_embedding_metrics(embedding_df, output_dir, plot_index, args.dpi)

    output_dir.mkdir(parents=True, exist_ok=True)
    index_path = output_dir / "plot_index.json"
    index_path.write_text(json.dumps(plot_index, indent=2, sort_keys=True), encoding="utf-8")
    print(f"generated {len(plot_index)} plots")
    print(f"wrote {index_path}")
    for item in plot_index:
        print(item["file"])


if __name__ == "__main__":
    main()
