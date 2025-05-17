import argparse, json, re, sys
from collections import Counter, defaultdict
from pathlib import Path
from bs4 import BeautifulSoup
from nltk.stem import PorterStemmer
from tqdm import tqdm

stemmer = PorterStemmer()
TOKEN_RE = re.compile(r"[A-Za-z0-9']+")

def tokenize(html: str):
    text = BeautifulSoup(html, "lxml").get_text(" ")
    for m in TOKEN_RE.finditer(text):
        yield stemmer.stem(m.group(0).lower())

def _flush(inv, path: Path):
    with open(path, "a", encoding="utf-8") as f:
        for term, posts in inv.items():
            f.write(json.dumps({"t": term, "p": posts}) + "\n")

def build(data_root: Path, out_dir: Path, flush_every: int):
    out_dir.mkdir(parents=True, exist_ok=True)
    inv: defaultdict[str, Counter[int]] = defaultdict(Counter)
    docmap, vocab = {}, set()
    doc_id = 0

    files = list(data_root.rglob("*.json"))
    for fp in tqdm(files, unit="doc"):
        page = json.load(fp.open(encoding="utf-8", errors="ignore"))
        html = page.get("content", "")
        if not html:
            continue
        tf = Counter(tokenize(html))
        if not tf:
            continue
        docmap[doc_id] = page.get("url", str(fp))
        for term, freq in tf.items():
            inv[term][doc_id] = freq
        vocab.update(tf)
        doc_id += 1
        if doc_id % flush_every == 0:
            _flush(inv, out_dir / f"part_{doc_id//flush_every}.jsonl")
            inv.clear()

    _flush(inv, out_dir / "part_final.jsonl")
    stats = {
        "n_docs": len(docmap),
        "n_tokens": len(vocab),
        "index_size_kb": sum(f.stat().st_size for f in out_dir.glob("*.jsonl")) // 1024,
    }
    (out_dir / "docmap.json").write_text(json.dumps(docmap))
    (out_dir / "stats.json").write_text(json.dumps(stats, indent=2))
    print("Finished!", stats)

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--data", required=True, type=Path)
    p.add_argument("--out", required=True, type=Path)
    p.add_argument("--flush", type=int, default=5000)
    args = p.parse_args()
    if not args.data.exists():
        sys.exit("Data folder not found.")
    build(args.data, args.out, args.flush)
