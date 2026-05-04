#!/usr/bin/env python3
"""
Architecture diagram for the distributed semantic RAG system.
Single-panel: binary routing tree overview.
"""

from __future__ import annotations

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch
from dataclasses import dataclass
from typing import Optional


# ── Palette ───────────────────────────────────────────────────────────────────

VM_COLORS = [
    "#F48FB1", "#FFCC80", "#C5E1A5", "#80DEEA",
    "#9FA8DA", "#FFAB91", "#80CBC4", "#CE93D8",
]

SPLIT_COLOR = "#90A4AE"
ROOT_COLOR  = "#F4A261"
QUERY_COLOR = "#E63946"
MATCH_COLOR = "#2A9D8F"
EDGE_COLOR  = "#CFD8DC"
TEXT_COLOR  = "#37474F"
BG          = "#FFFFFF"


# ── Tree model ────────────────────────────────────────────────────────────────

@dataclass
class TreeNode:
    id: str
    depth: int
    parent_id: Optional[str]
    left_id:   Optional[str]
    right_id:  Optional[str]
    is_leaf:   bool
    vm_id:     Optional[int] = None
    leaf_idx:  Optional[int] = None


def build_balanced_tree(n_leaves: int = 16) -> tuple[dict[str, TreeNode], str]:
    nodes: dict[str, TreeNode] = {}
    counter = [0]
    leaf_counter = [0]
    max_depth = int(np.log2(n_leaves))

    def _make_id() -> str:
        i = counter[0]; counter[0] += 1
        return f"n{i}"

    def _build(depth: int, parent_id: Optional[str]) -> str:
        nid = _make_id()
        if depth == max_depth:
            lidx = leaf_counter[0]; leaf_counter[0] += 1
            nodes[nid] = TreeNode(
                id=nid, depth=depth, parent_id=parent_id,
                left_id=None, right_id=None, is_leaf=True,
                vm_id=lidx // 2, leaf_idx=lidx,
            )
            return nid
        left_id  = _build(depth + 1, nid)
        right_id = _build(depth + 1, nid)
        nodes[nid] = TreeNode(
            id=nid, depth=depth, parent_id=parent_id,
            left_id=left_id, right_id=right_id, is_leaf=False,
        )
        return nid

    root_id = _build(0, None)
    return nodes, root_id


def compute_layout(nodes: dict[str, TreeNode], root_id: str) -> dict[str, tuple[float, float]]:
    layout: dict[str, list[float]] = {}
    max_depth = max(n.depth for n in nodes.values())
    y_scale = 3.2

    for n in nodes.values():
        if n.is_leaf:
            layout[n.id] = [float(n.leaf_idx), -max_depth * y_scale]

    def _assign_x(nid: str) -> float:
        node = nodes[nid]
        if node.is_leaf:
            return layout[nid][0]
        xl = _assign_x(node.left_id)
        xr = _assign_x(node.right_id)
        x = (xl + xr) / 2.0
        layout[nid] = [x, -node.depth * y_scale]
        return x

    _assign_x(root_id)
    return {nid: (pos[0], pos[1]) for nid, pos in layout.items()}


def get_query_path(
    nodes: dict[str, TreeNode],
    root_id: str,
    target_leaf_idx: int = 12,
) -> list[str]:
    def _dfs(nid: str, acc: list[str]) -> list[str] | None:
        node = nodes[nid]
        acc = acc + [nid]
        if node.is_leaf:
            return acc if node.leaf_idx == target_leaf_idx else None
        return _dfs(node.left_id, acc) or _dfs(node.right_id, acc)

    result = _dfs(root_id, [])
    assert result is not None
    return result


# ── Main diagram ──────────────────────────────────────────────────────────────

def draw_tree(
    ax: plt.Axes,
    nodes: dict[str, TreeNode],
    layout: dict[str, tuple[float, float]],
    query_path: list[str],
    root_id: str,
) -> None:
    ax.set_facecolor(BG)
    ax.set_axis_off()

    query_edges    = set(zip(query_path, query_path[1:]))
    query_path_set = set(query_path)
    leaf_nodes     = sorted([n for n in nodes.values() if n.is_leaf], key=lambda n: n.leaf_idx)

    # VM grouping boxes
    for vm_id in range(8):
        vm_leaves = [n for n in leaf_nodes if n.vm_id == vm_id]
        xs = [layout[n.id][0] for n in vm_leaves]
        y_leaf = layout[vm_leaves[0].id][1]
        x_min, x_max = min(xs) - 0.60, max(xs) + 0.60
        ax.add_patch(FancyBboxPatch(
            (x_min, y_leaf - 0.80), x_max - x_min, 1.70,
            boxstyle="round,pad=0.0",
            facecolor=VM_COLORS[vm_id], edgecolor="#AAAAAA",
            linewidth=1.0, alpha=0.38, zorder=0,
        ))
        ax.text(
            (x_min + x_max) / 2, y_leaf - 0.98,
            f"VM {vm_id + 1}",
            ha="center", va="top", fontsize=14, color="#555555",
        )

    # Tree edges
    for nid, node in nodes.items():
        if node.is_leaf:
            continue
        x0, y0 = layout[nid]
        for child_id in (node.left_id, node.right_id):
            x1, y1 = layout[child_id]
            on_path = (nid, child_id) in query_edges
            if on_path:
                ax.plot([x0, x1], [y0, y1], color=QUERY_COLOR, lw=10, alpha=0.13,
                        zorder=1, solid_capstyle="round")
                ax.plot([x0, x1], [y0, y1], color=QUERY_COLOR, lw=2.8, alpha=0.95,
                        zorder=2, solid_capstyle="round")
                mx, my = (x0 + x1) / 2, (y0 + y1) / 2
                dx, dy = x1 - x0, y1 - y0
                ax.annotate(
                    "", xy=(mx + dx * 0.01, my + dy * 0.01),
                    xytext=(mx - dx * 0.01, my - dy * 0.01),
                    arrowprops=dict(arrowstyle="-|>", color=QUERY_COLOR,
                                    lw=0, mutation_scale=18),
                    zorder=3,
                )
            else:
                ax.plot([x0, x1], [y0, y1], color=EDGE_COLOR, lw=1.3, zorder=1)

    # Split nodes
    for nid, node in nodes.items():
        if node.is_leaf or node.depth == 0:
            continue
        x, y = layout[nid]
        ax.add_patch(mpatches.Circle(
            (x, y), 0.44, color=SPLIT_COLOR, ec="#607D8B", lw=1.0, zorder=4,
        ))

    # Root — gold diamond
    rx, ry = layout[root_id]
    s = 0.72
    ax.add_patch(mpatches.Polygon(
        [[rx, ry + s], [rx + s * 0.72, ry], [rx, ry - s], [rx - s * 0.72, ry]],
        closed=True, facecolor=ROOT_COLOR, edgecolor="#C47A3A", lw=2.0, zorder=5,
    ))
    ax.text(rx, ry + 0.92, "Root / Leader", ha="center", va="bottom",
            fontsize=15, color="#7A4500", fontweight="bold")

    # Leaf nodes
    for node in leaf_nodes:
        x, y = layout[node.id]
        on_path = node.id in query_path_set
        ax.add_patch(mpatches.Circle(
            (x, y), 0.54,
            color=VM_COLORS[node.vm_id],
            ec=QUERY_COLOR if on_path else "#9E9E9E",
            lw=2.8 if on_path else 0.8, zorder=4,
        ))
        ax.text(x, y, "▤", ha="center", va="center", fontsize=12, color="#37474F", zorder=5)
        ax.text(x, y - 0.72, f"C{node.leaf_idx + 1}", ha="center", va="top",
                fontsize=12, color="#546E7A", fontweight="bold")

    # Query annotation
    ax.annotate(
        '"What is X?"', xy=(rx - s * 0.72, ry),
        xytext=(rx - 6.0, ry + 3.2),
        ha="center", fontsize=16, color=QUERY_COLOR, fontweight="bold",
        arrowprops=dict(
            arrowstyle="-|>", color=QUERY_COLOR, lw=2.4, mutation_scale=18,
            connectionstyle="arc3,rad=-0.25",
        ),
    )

    # Legend
    lx, ly = 16.5, -0.2
    ax.add_patch(FancyBboxPatch(
        (lx - 0.3, ly - 6.2), 4.0, 6.6,
        boxstyle="round,pad=0.2",
        facecolor="white", edgecolor="#B0BEC5", lw=1.0, zorder=10,
    ))
    ax.text(lx + 1.7, ly - 0.18, "Legend", ha="center", va="top",
            fontsize=15, fontweight="bold", color=TEXT_COLOR, zorder=11)

    for i, (fc, ec, label) in enumerate([
        (ROOT_COLOR,   "#C47A3A", "Root / Leader"),
        (SPLIT_COLOR,  "#607D8B", "Split node"),
        (VM_COLORS[3], "#9E9E9E", "Leaf container"),
    ]):
        yi = ly - 1.6 - i * 1.35
        ax.add_patch(mpatches.Circle((lx + 0.35, yi), 0.36, color=fc, ec=ec, lw=0.8, zorder=11))
        ax.text(lx + 0.90, yi, label, va="center", fontsize=13, color=TEXT_COLOR, zorder=12)

    yi_q = ly - 1.6 - 3 * 1.35
    ax.plot([lx, lx + 0.80], [yi_q, yi_q], color=QUERY_COLOR, lw=3.0,
            solid_capstyle="round", zorder=11)
    ax.annotate("", xy=(lx + 0.81, yi_q), xytext=(lx + 0.79, yi_q),
                arrowprops=dict(arrowstyle="-|>", color=QUERY_COLOR,
                                lw=0, mutation_scale=13), zorder=12)
    ax.text(lx + 0.90, yi_q, "Query traversal", va="center", fontsize=13,
            color=TEXT_COLOR, zorder=12)

    xs_all = [p[0] for p in layout.values()]
    ys_all = [p[1] for p in layout.values()]
    ax.set_xlim(min(xs_all) - 1.8, max(xs_all) + 5.8)
    ax.set_ylim(min(ys_all) - 2.2, max(ys_all) + 4.0)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    tree, root_id = build_balanced_tree(16)
    layout        = compute_layout(tree, root_id)
    query_path    = get_query_path(tree, root_id, target_leaf_idx=12)

    fig, ax = plt.subplots(figsize=(26, 18))
    fig.set_facecolor(BG)
    fig.subplots_adjust(left=0.01, right=0.99, top=0.93, bottom=0.06)

    draw_tree(ax, tree, layout, query_path, root_id)

    ax.text(0.5, -0.022,
            "Binary routing tree.  32 containers across 8 VMs form a 4-level k-means binary tree.  "
            "Root/leader (gold ◆) and split nodes (blue ●) route queries by comparing the query "
            "embedding to left/right k-means centroids.  "
            "Leaf containers (VM-colored ▤) hold documents.  Red path = example query traversal.",
            transform=ax.transAxes, ha="center", va="top",
            fontsize=12, color="#546E7A", style="italic", wrap=True)

    fig.suptitle(
        "Distributed Semantic RAG — Routing Tree Architecture",
        fontsize=20, fontweight="bold", color=TEXT_COLOR, y=0.975,
    )

    fig.savefig("architecture_diagram.png", dpi=150, bbox_inches="tight", facecolor=BG)
    print("Saved architecture_diagram.png")
    fig.savefig("architecture_diagram.svg", bbox_inches="tight", facecolor=BG)
    print("Saved architecture_diagram.svg")


if __name__ == "__main__":
    main()
