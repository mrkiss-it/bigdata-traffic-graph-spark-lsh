"""Trích xuất và chuẩn hóa đồ thị mạng lưới đường cụm Ngã tư Hàng Xanh (Nhóm 11 - [H3]).

Môn học: Phân tích dữ liệu lớn (Big Data Analytics) — CITD / UIT
Đề tài: Pipeline xử lý dữ liệu lớn giao thông đồ thị không–thời gian bằng PySpark & LSH/ANN
Giảng viên hướng dẫn: TS. Hà Minh Tân

Quy trình:
1. Tải hoặc kế thừa đồ thị đường lái xe (`network_type='drive'`, `simplify=True`) trong bán kính 1000m
   quanh Ngã tư Hàng Xanh gồm 6 trục đường trọng điểm của đề cương:
   - Xô Viết Nghệ Tĩnh
   - Đinh Bộ Lĩnh
   - Bạch Đằng
   - Điện Biên Phủ
   - Nguyễn Gia Trí (D2)
   - Ung Văn Khiêm
   (kèm Cầu vượt Hàng Xanh, Cầu Đinh Bộ Lĩnh, Cầu Sơn, Cầu Văn Thánh liên thông).
2. Chuẩn hóa hệ tọa độ phẳng mét (UTM Zone 48N) và tọa độ địa lý WGS-84 (EPSG:4326).
3. Rút gọn thành đồ thị có hướng gồm 20 nút giao trọng điểm (nằm trong ngưỡng 15–25 nút), gán nhãn chuẩn hóa:
   `node_id` (1..20) và `node_code` (`HX_01`..`HX_20`) theo khoảng cách từ tâm Hàng Xanh.
4. Tính toán ma trận kề nhị phân (Binary Adjacency) và ma trận trọng số Gaussian (Gaussian Kernel Adjacency)
   theo chuẩn DCRNN / Spatio-Temporal Graph (Li et al., 2018):
       W_ij = exp(-dist(v_i, v_j)^2 / sigma^2) với dist tính bằng Dijkstra trên độ dài cạnh
       và ngưỡng thưa epsilon = 0.1.
5. Xuất các sản phẩm bàn giao vào thư mục `01_data_ingestion/`:
   - `hang_xanh_nodes.csv` (20 nút giao trọng điểm)
   - `hang_xanh_edges.csv` (37 cạnh đường có hướng)
   - `hang_xanh_adjacency_gaussian.csv` (ma trận W_ij)
   - `hang_xanh_adjacency_binary.csv` (ma trận kề nhị phân)
   - `hang_xanh.graphml` (cấu trúc đồ thị chuẩn GraphML)
"""

from __future__ import annotations

import argparse
import math
import pathlib
import sys
from typing import Any

import networkx as nx
import numpy as np
import pandas as pd
from shapely.geometry import Point

# Thiết lập tham số mặc định
DEFAULT_CENTER = (10.8012, 106.7114)  # Ngã tư Hàng Xanh (lat, lon)
DEFAULT_DIST_M = 1000

MAJOR_ROADS_FILTER = (
    '["highway"~"trunk|trunk_link|primary|primary_link|secondary|secondary_link|tertiary|tertiary_link"]'
)

TARGET_AXIS_KEYWORDS = (
    "Xô Viết Nghệ Tĩnh",
    "Đinh Bộ Lĩnh",
    "Bạch Đằng",
    "Điện Biên Phủ",
    "Nguyễn Gia Trí",
    "Ung Văn Khiêm",
    "Hàng Xanh",
    "Cầu Đinh Bộ Lĩnh",
    "Cầu Sơn",
    "Cầu Văn Thánh",
)

HIGHWAY_PRIORITY = {
    "trunk": 0,
    "trunk_link": 1,
    "primary": 2,
    "primary_link": 3,
    "secondary": 4,
    "secondary_link": 5,
    "tertiary": 6,
    "tertiary_link": 7,
}

SEED_FALLBACK_PATHS = (
    pathlib.Path(__file__).resolve().parent / "hang_xanh.graphml",
    pathlib.Path(r"D:\Trí tuệ nhân tạo\HK3\Máy học nâng cao\Đồ án\01_data_sample\hang_xanh.graphml"),
)


def _flatten_values(val: Any) -> list[str]:
    """Chuẩn hóa thuộc tính OSM thành danh sách chuỗi."""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return []
    if isinstance(val, (list, tuple, set)):
        out: list[str] = []
        for item in val:
            out.extend(_flatten_values(item))
        return out
    return [str(val).strip()]


def _unique_ordered(items: list[str]) -> list[str]:
    """Loại bỏ trùng lặp giữ nguyên thứ tự xuất hiện ban đầu."""
    seen: set[str] = set()
    res: list[str] = []
    for item in items:
        if item and item not in seen:
            seen.add(item)
            res.append(item)
    return res


def matches_target_axes(name_val: Any) -> bool:
    """Kiểm tra cạnh có thuộc 6 trục đường mục tiêu quanh Hàng Xanh (loại trừ Hẻm)."""
    vals = _flatten_values(name_val)
    if not vals:
        return False
    joined = " | ".join(vals)
    if "hẻm" in joined.lower():
        return False
    lowered = joined.lower()
    return any(kw.lower() in lowered for kw in TARGET_AXIS_KEYWORDS)


def fetch_raw_graph_osmnx(
    center: tuple[float, float] = DEFAULT_CENTER,
    dist_m: int = DEFAULT_DIST_M,
) -> nx.MultiDiGraph:
    """Tải đồ thị OSMnx quanh Hàng Xanh với bộ lọc trục đường chính."""
    import osmnx as ox

    print(f"-> Đang kết nối OSMnx tải đồ thị quanh tâm {center} (bán kính {dist_m}m)...")
    ox.settings.use_cache = True
    ox.settings.log_console = False

    G = ox.graph_from_point(
        center,
        dist=dist_m,
        network_type="drive",
        simplify=True,
        custom_filter=MAJOR_ROADS_FILTER,
    )
    print(f"   Tải thành công: {G.number_of_nodes()} nút, {G.number_of_edges()} cạnh thô.")
    return G


def filter_target_axes(G: nx.MultiDiGraph) -> nx.MultiDiGraph:
    """Lọc đồ thị giữ lại các cạnh thuộc 6 trục đường chính và thành phần liên thông lớn nhất."""
    axis_edges = [
        (u, v, k)
        for u, v, k, data in G.edges(keys=True, data=True)
        if matches_target_axes(data.get("name"))
    ]
    G_axes = G.edge_subgraph(axis_edges).copy()
    axis_nodes = set(G_axes.nodes())

    sub_edges = [
        (u, v, k)
        for u, v, k, data in G.edges(keys=True, data=True)
        if u in axis_nodes
        and v in axis_nodes
        and (not _flatten_values(data.get("name")) or matches_target_axes(data.get("name")))
    ]
    G_sub = G.edge_subgraph(sub_edges).copy()
    if G_sub.number_of_nodes() == 0:
        raise ValueError("Đồ thị sau lọc 6 trục đường rỗng.")

    largest_wcc = max(nx.weakly_connected_components(G_sub), key=len)
    return G_sub.subgraph(largest_wcc).copy()


def merge_close_nodes(G_utm: nx.MultiDiGraph, max_dist_m: float = 65.0) -> nx.MultiDiGraph:
    """Gộp các cặp nút đối chiều quá gần nhau trên đại lộ để chuẩn hóa nút giao 2 chiều."""
    nodes = sorted(G_utm.nodes())
    proximity = nx.Graph()
    proximity.add_nodes_from(nodes)
    for idx, u in enumerate(nodes):
        ux, uy = G_utm.nodes[u]["x"], G_utm.nodes[u]["y"]
        for v in nodes[idx + 1 :]:
            vx, vy = G_utm.nodes[v]["x"], G_utm.nodes[v]["y"]
            if math.hypot(ux - vx, uy - vy) <= max_dist_m:
                proximity.add_edge(u, v)

    merged = nx.MultiDiGraph()
    merged.graph.update(G_utm.graph)
    node_map: dict[Any, int] = {}

    for new_id, comp in enumerate(sorted(nx.connected_components(proximity), key=lambda c: min(c))):
        comp_list = sorted(comp)
        merged.add_node(
            new_id,
            x=sum(G_utm.nodes[n]["x"] for n in comp_list) / len(comp_list),
            y=sum(G_utm.nodes[n]["y"] for n in comp_list) / len(comp_list),
        )
        for old_n in comp_list:
            node_map[old_n] = new_id

    for u, v, _, data in G_utm.edges(keys=True, data=True):
        nu, nv = node_map[u], node_map[v]
        if nu == nv:
            continue
        nd = dict(data)
        nd.pop("geometry", None)
        merged.add_edge(nu, nv, **nd)

    return merged


def contract_short_splits(G_utm: nx.MultiDiGraph, max_combined_len: float = 450.0) -> nx.MultiDiGraph:
    """Rút gọn nút trung gian bậc 2 trên cùng một tuyến đường."""
    H = G_utm.copy()
    changed = True
    while changed:
        changed = False
        for node in sorted(list(H.nodes())):
            nbrs = (set(H.predecessors(node)) | set(H.successors(node))) - {node}
            if len(nbrs) != 2:
                continue

            inc = list(H.in_edges(node, data=True))
            out = list(H.out_edges(node, data=True))
            if not inc or not out:
                continue

            roads: set[str] = set()
            for _, _, d in inc + out:
                roads.update(_flatten_values(d.get("name")))
            if len(roads) > 1:
                continue

            candidate_pairs = [
                (u, v, d1, d2) for u, _, d1 in inc for _, v, d2 in out if u != v
            ]
            if not candidate_pairs:
                continue
            max_len = max(
                float(d1.get("length", 0.0)) + float(d2.get("length", 0.0))
                for _, _, d1, d2 in candidate_pairs
            )
            if max_len > max_combined_len:
                continue

            H.remove_node(node)
            for u, v, d1, d2 in candidate_pairs:
                nd = dict(d1)
                nd["length"] = float(d1.get("length", 0.0)) + float(d2.get("length", 0.0))
                nd.pop("geometry", None)
                H.add_edge(u, v, **nd)
            changed = True
            break
    return H


def dedup_and_normalize_edges(G_utm: nx.MultiDiGraph) -> nx.MultiDiGraph:
    """Hợp nhất các cạnh song song cùng cặp nút (u -> v) và bảo toàn thuộc tính graph CRS."""
    clean = nx.MultiDiGraph()
    clean.graph.update(G_utm.graph)
    for n, d in G_utm.nodes(data=True):
        clean.add_node(n, **d)

    grouped: dict[tuple[Any, Any], list[dict[str, Any]]] = {}
    for u, v, _, d in G_utm.edges(keys=True, data=True):
        grouped.setdefault((u, v), []).append(d)

    for (u, v), ds in sorted(grouped.items()):
        names = _unique_ordered([n for d in ds for n in _flatten_values(d.get("name"))])
        road_name = " / ".join(names) if names else "Đoạn nối Hàng Xanh"
        highways = _unique_ordered([str(d.get("highway", "primary")) for d in ds if d.get("highway")])
        highway_str = highways[0] if highways else "primary"
        lanes_vals = [int(lv) for d in ds for lv in _flatten_values(d.get("lanes")) if lv.isdigit()]
        lanes = max(lanes_vals) if lanes_vals else 2
        avg_length = round(sum(float(d.get("length", 0.0)) for d in ds) / len(ds), 2)
        oneway = all(bool(d.get("oneway", False)) for d in ds)

        attrs: dict[str, Any] = {
            "name": road_name,
            "highway": highway_str,
            "lanes": lanes,
            "length": avg_length,
            "oneway": oneway,
            "maxspeed": "60",
        }
        clean.add_edge(u, v, **attrs)

    return clean


def relabel_by_center_distance(
    G_utm: nx.MultiDiGraph, center: tuple[float, float] = DEFAULT_CENTER
) -> tuple[nx.MultiDiGraph, pd.DataFrame]:
    """Gán nhãn nút theo khoảng cách tăng dần từ tâm Hàng Xanh (HX_01 .. HX_N)."""
    import osmnx as ox

    G_ll = ox.project_graph(G_utm, to_crs="EPSG:4326")

    center_geom, _ = ox.projection.project_geometry(
        Point(center[1], center[0]),
        crs="EPSG:4326",
        to_crs=G_utm.graph["crs"],
    )
    cx, cy = center_geom.x, center_geom.y

    ranked = sorted(
        G_utm.nodes(),
        key=lambda n: (
            math.hypot(G_utm.nodes[n]["x"] - cx, G_utm.nodes[n]["y"] - cy),
            -G_ll.nodes[n]["y"],
            G_ll.nodes[n]["x"],
        ),
    )
    id_map = {old_n: idx for idx, old_n in enumerate(ranked, start=1)}

    G_final = nx.MultiDiGraph()
    G_final.graph["crs"] = "EPSG:4326"
    G_final.graph["name"] = "Hang_Xanh_Key_Corridors"

    node_rows: list[dict[str, Any]] = []
    for old_n in ranked:
        nid = id_map[old_n]
        node_code = f"HX_{nid:02d}"
        lat = round(float(G_ll.nodes[old_n]["y"]), 6)
        lon = round(float(G_ll.nodes[old_n]["x"]), 6)
        utm_x = round(float(G_utm.nodes[old_n]["x"]), 2)
        utm_y = round(float(G_utm.nodes[old_n]["y"]), 2)
        dist_center_m = round(math.hypot(utm_x - cx, utm_y - cy), 1)

        inc_edges = list(G_utm.in_edges(old_n, data=True))
        out_edges = list(G_utm.out_edges(old_n, data=True))
        roads = _unique_ordered(
            [nm for _, _, d in inc_edges + out_edges for nm in _flatten_values(d.get("name")) if nm != "Đoạn nối Hàng Xanh"]
        )
        hw_classes = _unique_ordered(
            [str(d.get("highway")) for _, _, d in inc_edges + out_edges if d.get("highway")]
        )

        G_final.add_node(
            nid,
            x=lon,
            y=lat,
            lat=lat,
            lon=lon,
            node_code=node_code,
            roads=" | ".join(roads),
            dist_center_m=dist_center_m,
            utm_x=utm_x,
            utm_y=utm_y,
            highway_classes=" | ".join(hw_classes),
        )
        node_rows.append(
            {
                "node_id": nid,
                "node_code": node_code,
                "lat": lat,
                "lon": lon,
                "roads": " | ".join(roads),
                "highway_classes": " | ".join(hw_classes),
                "in_degree": len(inc_edges),
                "out_degree": len(out_edges),
                "dist_center_m": dist_center_m,
                "utm_x": utm_x,
                "utm_y": utm_y,
            }
        )

    for u, v, _, d in sorted(G_utm.edges(keys=True, data=True), key=lambda e: (id_map[e[0]], id_map[e[1]])):
        nu, nv = id_map[u], id_map[v]
        G_final.add_edge(nu, nv, **d)

    return G_final, pd.DataFrame(node_rows)


def build_from_preprocessed_graph(G: nx.MultiDiGraph) -> tuple[nx.MultiDiGraph, pd.DataFrame, pd.DataFrame]:
    """Tái tạo bảng nodes và edges từ đồ thị GraphML đã chuẩn hóa 20 nút."""
    nodes = []
    for n, d in sorted(G.nodes(data=True), key=lambda x: int(x[0])):
        nid = int(n)
        nodes.append({
            "node_id": nid,
            "node_code": d.get("node_code", f"HX_{nid:02d}"),
            "lat": round(float(d.get("lat", d.get("y"))), 6),
            "lon": round(float(d.get("lon", d.get("x"))), 6),
            "roads": d.get("roads", ""),
            "highway_classes": d.get("highway_classes", "primary"),
            "in_degree": G.in_degree(n),
            "out_degree": G.out_degree(n),
            "dist_center_m": round(float(d.get("dist_center_m", 0.0)), 1),
            "utm_x": round(float(d.get("utm_x", 0.0)), 2),
            "utm_y": round(float(d.get("utm_y", 0.0)), 2),
        })
    nodes_df = pd.DataFrame(nodes)

    edge_set = {(int(u), int(v)) for u, v in G.edges()}
    edges = []
    for idx, (u, v, d) in enumerate(
        sorted(G.edges(data=True), key=lambda e: (int(e[0]), int(e[1]))), start=1
    ):
        nu, nv = int(u), int(v)
        edges.append({
            "edge_id": f"E_{idx:02d}",
            "u": nu,
            "v": nv,
            "u_code": f"HX_{nu:02d}",
            "v_code": f"HX_{nv:02d}",
            "name": d.get("name", "Đường Hàng Xanh"),
            "highway": d.get("highway", "primary"),
            "lanes": int(d.get("lanes", 1)),
            "length": round(float(d.get("length", 100.0)), 2),
            "oneway": bool(d.get("oneway", True)),
            "bidirectional_pair": (nv, nu) in edge_set,
            "maxspeed": str(d.get("maxspeed", "60")),
        })
    edges_df = pd.DataFrame(edges)
    return G, nodes_df, edges_df


def compute_gaussian_adjacency(
    G: nx.MultiDiGraph,
    nodes_df: pd.DataFrame,
    epsilon: float = 0.1,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, float]]:
    """Tính toán ma trận kề nhị phân và ma trận trọng số Gaussian theo chuẩn DCRNN.

    Công thức:
        W_ij = exp(-dist(v_i, v_j)^2 / sigma^2) với dist tính bằng Dijkstra trên độ dài cạnh
        Nếu dist = inf hoặc W_ij < epsilon: W_ij = 0
        Đường chéo W_ii = 1.0
    """
    n = len(nodes_df)
    node_ids = nodes_df["node_id"].tolist()
    node_codes = nodes_df["node_code"].tolist()

    # Tạo đồ thị đơn hướng cho Dijkstra
    G_simple = nx.DiGraph()
    for nid in node_ids:
        G_simple.add_node(nid)
    for u, v, d in G.edges(data=True):
        nu, nv = int(u), int(v)
        l = float(d.get("length", 100.0))
        if G_simple.has_edge(nu, nv):
            G_simple[nu][nv]["length"] = min(G_simple[nu][nv]["length"], l)
        else:
            G_simple.add_edge(nu, nv, length=l)

    # 1. Ma trận kề nhị phân
    adj_binary_df = nx.to_pandas_adjacency(G_simple, nodelist=node_ids, weight=None, dtype=int)
    adj_binary_df.index = node_codes
    adj_binary_df.columns = node_codes

    # 2. Ma trận khoảng cách mạng lưới (Dijkstra)
    dist_matrix = np.full((n, n), np.inf)
    for i_idx, u in enumerate(node_ids):
        dist_matrix[i_idx, i_idx] = 0.0
        lengths = nx.single_source_dijkstra_path_length(G_simple, u, weight="length")
        for v, d in lengths.items():
            if v in node_ids:
                j_idx = node_ids.index(v)
                dist_matrix[i_idx, j_idx] = d

    # 3. Tính độ lệch chuẩn sigma của các khoảng cách hữu hạn khác 0
    finite_dists = dist_matrix[np.isfinite(dist_matrix) & (dist_matrix > 0)]
    if len(finite_dists) > 0:
        sigma = float(np.std(finite_dists))
        mean_dist = float(np.mean(finite_dists))
    else:
        sigma = 500.0
        mean_dist = 500.0

    # 4. Tính ma trận trọng số Gaussian
    weights = np.zeros((n, n), dtype=float)
    for i in range(n):
        for j in range(n):
            d = dist_matrix[i, j]
            if np.isfinite(d):
                val = float(np.exp(- (d ** 2) / (sigma ** 2)))
                if val >= epsilon:
                    weights[i, j] = round(val, 4)

    adj_gaussian_df = pd.DataFrame(weights, index=node_codes, columns=node_codes)

    stats = {
        "n_nodes": n,
        "n_finite_paths": int(len(finite_dists)),
        "mean_distance_m": round(mean_dist, 2),
        "std_distance_m": round(sigma, 2),
        "density_gaussian": round(float(np.count_nonzero(weights)) / (n * n), 4),
        "epsilon": epsilon,
    }
    return adj_binary_df, adj_gaussian_df, stats


def build_hang_xanh_graph(
    center: tuple[float, float] = DEFAULT_CENTER,
    dist_m: int = DEFAULT_DIST_M,
    consolidation_tolerance_m: float = 30.0,
    merge_dist_m: float = 65.0,
    max_contract_len_m: float = 450.0,
    seed_graphml: pathlib.Path | None = None,
    force_online: bool = False,
) -> tuple[nx.MultiDiGraph, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Pipeline trích xuất và chuẩn hóa đồ thị Hàng Xanh hoàn chỉnh."""
    import osmnx as ox

    # 1. Kiểm tra seed graphml trước nếu không bắt buộc online
    chosen_seed: pathlib.Path | None = None
    if seed_graphml is not None and seed_graphml.exists():
        chosen_seed = seed_graphml
    elif not force_online:
        for p in SEED_FALLBACK_PATHS:
            if p.exists():
                chosen_seed = p
                break

    if chosen_seed is not None:
        print(f"-> Nạp hạt giống đồ thị chuẩn từ: {chosen_seed}")
        G_loaded = ox.load_graphml(chosen_seed)
        if 15 <= G_loaded.number_of_nodes() <= 25 and any("node_code" in d for _, d in G_loaded.nodes(data=True)):
            print(f"   Đồ thị hạt giống chuẩn có {G_loaded.number_of_nodes()} nút giao trọng điểm. Tái lập ma trận kề...")
            G_final, nodes_df, edges_df = build_from_preprocessed_graph(G_loaded)
            adj_binary_df, adj_gaussian_df, stats = compute_gaussian_adjacency(G_final, nodes_df, epsilon=0.1)
            return G_final, nodes_df, edges_df, adj_binary_df, adj_gaussian_df, stats
        G_raw = G_loaded
    else:
        G_raw = fetch_raw_graph_osmnx(center=center, dist_m=dist_m)

    # 2. Quy trình trích xuất từ dữ liệu thô OSM
    G_sub = filter_target_axes(G_raw)
    G_utm = ox.project_graph(G_sub)

    G_cons = ox.consolidate_intersections(
        G_utm,
        rebuild_graph=True,
        tolerance=consolidation_tolerance_m,
        dead_ends=False,
    )
    G_merged = merge_close_nodes(G_cons, max_dist_m=merge_dist_m)
    G_contracted = contract_short_splits(G_merged, max_combined_len=max_contract_len_m)
    G_clean = dedup_and_normalize_edges(G_contracted)
    G_final, nodes_df = relabel_by_center_distance(G_clean, center=center)

    edge_set = {(u, v) for u, v in G_final.edges()}
    edge_rows: list[dict[str, Any]] = []
    for idx, (u, v, d) in enumerate(
        sorted(G_final.edges(data=True), key=lambda e: (e[0], e[1])),
        start=1,
    ):
        edge_rows.append(
            {
                "edge_id": f"E_{idx:02d}",
                "u": u,
                "v": v,
                "u_code": f"HX_{u:02d}",
                "v_code": f"HX_{v:02d}",
                "name": d.get("name", "Đường Hàng Xanh"),
                "highway": d.get("highway", "primary"),
                "lanes": int(d.get("lanes", 1)),
                "length": round(float(d.get("length", 100.0)), 2),
                "oneway": bool(d.get("oneway", True)),
                "bidirectional_pair": (v, u) in edge_set,
                "maxspeed": d.get("maxspeed", "60"),
            }
        )
    edges_df = pd.DataFrame(edge_rows)

    adj_binary_df, adj_gaussian_df, stats = compute_gaussian_adjacency(
        G_final, nodes_df, epsilon=0.1
    )

    return G_final, nodes_df, edges_df, adj_binary_df, adj_gaussian_df, stats


def export_graph_artifacts(
    output_dir: pathlib.Path,
    G_final: nx.MultiDiGraph,
    nodes_df: pd.DataFrame,
    edges_df: pd.DataFrame,
    adj_binary_df: pd.DataFrame,
    adj_gaussian_df: pd.DataFrame,
) -> dict[str, pathlib.Path]:
    """Xuất tất cả sản phẩm ra thư mục output_dir với kiểm toán số liệu nghiêm ngặt."""
    import osmnx as ox

    output_dir.mkdir(parents=True, exist_ok=True)
    n_nodes = len(nodes_df)

    if not (15 <= n_nodes <= 25):
        raise ValueError(f"Số nút sau khi rút gọn ({n_nodes}) nằm ngoài khoảng 15–25 mục tiêu!")

    paths = {
        "nodes": output_dir / "hang_xanh_nodes.csv",
        "edges": output_dir / "hang_xanh_edges.csv",
        "adj_gaussian": output_dir / "hang_xanh_adjacency_gaussian.csv",
        "adj_binary": output_dir / "hang_xanh_adjacency_binary.csv",
        "graphml": output_dir / "hang_xanh.graphml",
    }

    nodes_df.to_csv(paths["nodes"], index=False, encoding="utf-8-sig")
    edges_df.to_csv(paths["edges"], index=False, encoding="utf-8-sig")
    adj_gaussian_df.to_csv(paths["adj_gaussian"], encoding="utf-8-sig")
    adj_binary_df.to_csv(paths["adj_binary"], encoding="utf-8-sig")

    ox.save_graphml(G_final, filepath=paths["graphml"])
    return paths


def parse_args() -> argparse.Namespace:
    default_out = pathlib.Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(
        description="Trích xuất đồ thị đường bộ cụm Hàng Xanh (Nhóm 11 - [H3])."
    )
    parser.add_argument("--center-lat", type=float, default=DEFAULT_CENTER[0])
    parser.add_argument("--center-lon", type=float, default=DEFAULT_CENTER[1])
    parser.add_argument("--dist", type=int, default=DEFAULT_DIST_M)
    parser.add_argument("--tolerance", type=float, default=30.0)
    parser.add_argument("--merge-dist", type=float, default=65.0)
    parser.add_argument("--max-contract-len", type=float, default=450.0)
    parser.add_argument("--output-dir", type=pathlib.Path, default=default_out)
    parser.add_argument("--from-graphml", type=pathlib.Path, default=None)
    parser.add_argument("--online", action="store_true", help="Bắt buộc tải mới từ Overpass API")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    print("==============================================================================")
    print("PHÂN TÍCH DỮ LIỆU LỚN — TRÍCH XUẤT ĐỒ THỊ MẠNG LƯỚI ĐƯỜNG CỤM HÀNG XANH ([H3])")
    print(f"Tâm trích xuất: ({args.center_lat}, {args.center_lon}) | Bán kính: {args.dist} m")
    print("==============================================================================")

    G_final, nodes_df, edges_df, adj_binary, adj_gaussian, stats = build_hang_xanh_graph(
        center=(args.center_lat, args.center_lon),
        dist_m=args.dist,
        consolidation_tolerance_m=args.tolerance,
        merge_dist_m=args.merge_dist,
        max_contract_len_m=args.max_contract_len,
        seed_graphml=args.from_graphml,
        force_online=args.online,
    )

    paths = export_graph_artifacts(
        args.output_dir, G_final, nodes_df, edges_df, adj_binary, adj_gaussian
    )

    print(f"\n[Thành công] Đồ thị Hàng Xanh chuẩn hóa:")
    print(f"  • Số nút giao trọng điểm: {len(nodes_df)} nút (khoảng cách tâm: {nodes_df['dist_center_m'].min()}m -> {nodes_df['dist_center_m'].max()}m)")
    print(f"  • Số cạnh đường có hướng: {len(edges_df)} cạnh")
    print(f"  • Thống kê ma trận Gaussian: sigma = {stats['std_distance_m']} m, mật độ = {stats['density_gaussian'] * 100:.1f}%")
    print(f"\nSản phẩm bàn giao đã lưu tại:")
    for name, path in paths.items():
        print(f"  - [{name}]: {path}")


if __name__ == "__main__":
    main()
