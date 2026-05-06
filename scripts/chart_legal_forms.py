import gzip, json, os, collections
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

T_DECODE = {
    "A": "Aktiengesellschaft",
    "L": "Ausländische Niederlassung",
    "B": "Besondere Rechtsform",
    "E": "Einzelunternehmen",
    "N": "Genossenschaft",
    "G": "GmbH / SARL",
    "H": "Haupt von Gemeinderschaften",
    "I": "Institut des öffentlichen Rechts",
    "K": "Kollektivgesellschaft",
    "O": "Kommanditgesellschaft",
    "Z": "Schweizerische Zweigniederlassung",
    "S": "Stiftung",
    "V": "Verein",
}

COLOR_ASSIGNMENTS = [
    ("Aktiengesellschaft",              "#FFFF00"),
    ("Ausländische Niederlassung",      "#FFD700"),
    ("Schweizerische Zweigniederlassung","#FFBC00"),
    ("Besondere Rechtsform",            "#FFA500"),
    ("Einzelunternehmen",               "#FF8C00"),
    ("Genossenschaft",                  "#FF7F50"),
    ("GmbH / SARL",                     "#FF6347"),
    ("Haupt von Gemeinderschaften",     "#FF5555"),
    ("Institut des öffentlichen Rechts","#E63B8B"),
    ("Kollektivgesellschaft",           "#D632CE"),
    ("Kommanditgesellschaft",           "#C620BF"),
    ("Stiftung",                        "#8B4DB8"),
    ("Verein",                          "#7B5DAB"),
]
LABELS = [la for la, _ in COLOR_ASSIGNMENTS]
COLORS = [co for _, co in COLOR_ASSIGNMENTS]

CANTONS = [
    ("ZH","Zürich"), ("BE","Bern"), ("VD","Vaud"), ("AG","Aargau"),
    ("SG","St. Gallen"), ("GE","Genève"), ("TI","Ticino"), ("BS","Basel-Stadt"),
    ("VS","Valais"), ("LU","Luzern"), ("TG","Thurgau"), ("FR","Fribourg"),
    ("BL","Basel-Landschaft"), ("SO","Solothurn"), ("GR","Graubünden"),
    ("NE","Neuchâtel"), ("SZ","Schwyz"), ("ZG","Zug"), ("JU","Jura"),
    ("SH","Schaffhausen"), ("AR","App. Ausserrhoden"), ("NW","Nidwalden"),
    ("OW","Obwalden"), ("GL","Glarus"), ("AI","App. Innerrhoden"), ("UR","Uri"),
]

def load_canton(canton_id):
    meta_path = os.path.join(DATA_DIR, f"{canton_id}.json")
    with open(meta_path) as f:
        meta = json.load(f)

    chunks = []
    if "chunks" in meta:
        for fname in meta["chunks"]:
            with gzip.open(os.path.join(DATA_DIR, fname)) as f:
                chunks.append(json.load(f))
    else:
        for i in range(meta["_chunks"]):
            with gzip.open(os.path.join(DATA_DIR, f"{canton_id}_{i}.json.gz")) as f:
                chunks.append(json.load(f))

    counts = collections.Counter()
    for chunk in chunks:
        for code in chunk.get("t", []):
            label = T_DECODE.get(code, code)
            counts[label] += 1
    return counts

print("Loading canton data...")
stats = {}
for cid, cname in CANTONS:
    print(f"  {cid}...", end=" ", flush=True)
    stats[cid] = load_canton(cid)
    print(f"{sum(stats[cid].values()):,}")

# Build matrix: cantons (rows) × labels (cols), as percentages
matrix = np.zeros((len(CANTONS), len(LABELS)))
for i, (cid, _) in enumerate(CANTONS):
    total = sum(stats[cid].values())
    for j, label in enumerate(LABELS):
        matrix[i, j] = stats[cid].get(label, 0) / total * 100 if total else 0

# ── Plot ──────────────────────────────────────────────────────────────────────
BG = "#10172a"
fig, ax = plt.subplots(figsize=(16, 11))
fig.patch.set_facecolor(BG)
ax.set_facecolor(BG)

y = np.arange(len(CANTONS))
lefts = np.zeros(len(CANTONS))

bars = []
for j, (label, color) in enumerate(COLOR_ASSIGNMENTS):
    vals = matrix[:, j]
    b = ax.barh(y, vals, left=lefts, color=color, height=0.72, label=label)
    bars.append(b)
    for i, (val, left) in enumerate(zip(vals, lefts)):
        if val >= 3.5:
            ax.text(left + val / 2, i, f"{val:.0f}%", va="center", ha="center",
                    fontsize=6.5, color="#000", fontweight="500", alpha=0.75)
    lefts += vals

# Canton labels (right of bar)
for i, (cid, cname) in enumerate(CANTONS):
    total = sum(stats[cid].values())
    ax.text(101, i, f"{cid}  {total:,}", va="center", ha="left",
            color="rgba(255,255,255,0.55)" if False else "#aaa",
            fontsize=8.5)

ax.set_yticks(y)
ax.set_yticklabels([cname for _, cname in CANTONS], color="#bbb", fontsize=9)
ax.set_xlim(0, 132)
ax.set_xlabel("% of companies", color="#888", fontsize=10)
ax.tick_params(colors="#666", axis="x")
ax.set_xticks([0, 25, 50, 75, 100])
ax.set_xticklabels(["0%", "25%", "50%", "75%", "100%"], color="#666", fontsize=9)
for spine in ax.spines.values():
    spine.set_visible(False)
ax.xaxis.grid(True, color="#222", linewidth=0.5, zorder=0)
ax.set_axisbelow(True)
ax.tick_params(left=False, bottom=False)

ax.set_title("Legal Form Distribution by Canton", color="#ddd",
             fontsize=14, fontweight="600", pad=16)

# Legend below chart
legend_patches = [mpatches.Patch(color=c, label=l) for l, c in COLOR_ASSIGNMENTS]
legend = ax.legend(handles=legend_patches, loc="lower center",
                   bbox_to_anchor=(0.44, -0.18), ncol=4,
                   frameon=False, fontsize=8.5,
                   labelcolor="#aaa", handlelength=1.2, handleheight=0.9,
                   columnspacing=1.2, handletextpad=0.5)

ax.text(0.44, -0.27, "Source: opendata.swiss  ·  swissviz",
        transform=ax.transAxes, ha="center", color="#444", fontsize=8)

plt.tight_layout(rect=[0, 0.12, 1, 1])

out = os.path.join(os.path.dirname(__file__), "..", "legal_forms_by_canton.png")
plt.savefig(out, dpi=160, bbox_inches="tight", facecolor=BG)
print(f"\nSaved → {os.path.abspath(out)}")
