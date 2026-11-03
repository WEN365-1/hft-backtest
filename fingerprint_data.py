"""原始Parquet文件指纹。公开旧聚合值的精确算法；不读取或改写行情内容。"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

ALGORITHM = "sha256(concat(UTF8(relative_posix_path + NUL + decimal_bytes + NUL + lowercase_file_sha256 + LF))), lexicographic relative path order"


def fingerprint(root: str | Path) -> dict:
    root = Path(root)
    if not root.is_dir():
        raise ValueError(f"原始数据目录不存在: {root}")
    paths = sorted(root.rglob("*.parquet"), key=lambda p: p.relative_to(root).as_posix())
    manifest, digest, total = [], hashlib.sha256(), 0
    for path in paths:
        file_hash = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                file_hash.update(chunk)
        name, size, sha = path.relative_to(root).as_posix(), path.stat().st_size, file_hash.hexdigest()
        manifest.append({"file": name, "bytes": size, "sha256": sha})
        digest.update(f"{name}\0{size}\0{sha}\n".encode("utf-8"))
        total += size
    return {"summary": {"files": len(paths), "bytes": total,
                        "manifest_sha256": digest.hexdigest(), "algorithm": ALGORITHM},
            "manifest": manifest}


def verify(report: dict, expected: dict) -> None:
    if report["manifest"] != expected["manifest"]:
        raise ValueError("逐文件清单不一致：文件缺失、增加、字节数或内容发生变化")
    for key in ("files", "bytes", "manifest_sha256"):
        if report["summary"][key] != expected["summary"][key]:
            raise ValueError(f"汇总指纹不一致: {key}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", required=True)
    parser.add_argument("--output", help="完整逐文件清单仅留本地，不随老师包提交")
    parser.add_argument("--verify", help="与已有完整指纹清单对账，含兼容旧166fea聚合值的算法")
    args = parser.parse_args()
    report = fingerprint(args.data_root)
    if args.verify:
        verify(report, json.loads(Path(args.verify).read_text(encoding="utf-8")))
    if args.output:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report["summary"], ensure_ascii=False, indent=2))
