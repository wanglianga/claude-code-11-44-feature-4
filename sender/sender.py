#!/usr/bin/env python3
"""本地发送器 CLI：按可控顺序发送两类协议报文。

用法：
  python sender.py reset                     # 清空服务端数据
  python sender.py seed                      # 写入种子契约/关系/锚点
  python sender.py run <场景名|all>          # 按顺序执行场景
  python sender.py publish                   # 构建两个用途的发布版本
  python sender.py consume <name> <purpose>  # 模拟消费端按游标读完整版本

  python sender.py chain <场景|all>          # 观测链路缺口诊断演练
  python sender.py chain all --export DIR    # 仅导出可导入的虚构清单
  python sender.py chain <任意> --import-file F  # 导入导出文件并刷新诊断
  python sender.py backfill <场景>           # 补传该场景的迟到数据
  python sender.py gaps [--batch LOT] [--device PRB-2002] [--active]

接入场景：binary-main | binary-restart | json-basic | json-new-firmware | contract-revision
链路场景：baseline | platform-late | gateway-platform-loss | device-missing |
          device-to-gateway-loss | signals | scope-incomplete | plan-change |
          reboot-reset | duplicates-unordered | conflict | parse-backlog |
          shared-gateway-backfill
"""
import argparse
import json
import sys

import chain_scenarios
from scenarios import Client, SCENARIOS, run_all


def main():
    ap = argparse.ArgumentParser(description="冷链温控本地发送器（不依赖真实硬件）")
    ap.add_argument("--base", default="http://127.0.0.1:8000", help="服务端地址")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("reset")
    sub.add_parser("seed")
    p_run = sub.add_parser("run")
    p_run.add_argument("scenario", choices=[*SCENARIOS.keys(), "all"])
    sub.add_parser("publish")
    p_cons = sub.add_parser("consume")
    p_cons.add_argument("name")
    p_cons.add_argument("purpose", choices=["line_monitor", "batch_audit"])
    p_cons.add_argument("--size", type=int, default=5)

    p_chain = sub.add_parser("chain", help="观测链路缺口诊断场景")
    p_chain.add_argument("scenario", choices=[*chain_scenarios.SCENARIOS.keys(), "all"])
    p_chain.add_argument("--export", help="仅导出可导入的 JSON 清单，不调用服务")
    p_chain.add_argument("--import-file", help="导入之前导出的 JSON 清单并刷新诊断")

    p_gaps = sub.add_parser("gaps", help="按批次/设备查询缺口")
    p_gaps.add_argument("--batch", dest="cargo_batch_id")
    p_gaps.add_argument("--device")
    p_gaps.add_argument("--active", action="store_true")

    p_bf = sub.add_parser("backfill", help="补传指定场景的迟到数据")
    p_bf.add_argument("scenario")
    args = ap.parse_args()

    c = Client(args.base)
    if args.cmd == "reset":
        print(json.dumps(c.reset(), ensure_ascii=False))
    elif args.cmd == "seed":
        print(json.dumps(c.seed(), ensure_ascii=False))
    elif args.cmd == "run":
        out = run_all(c) if args.scenario == "all" else SCENARIOS[args.scenario](c)
        print(json.dumps(out, ensure_ascii=False, indent=2))
    elif args.cmd == "publish":
        for purpose in ("line_monitor", "batch_audit"):
            print(json.dumps(c.build_publication(purpose), ensure_ascii=False))
    elif args.cmd == "chain":
        if args.import_file:
            data = chain_scenarios.load(args.import_file)
            out = run_chain_bundle(c, data)
            print(json.dumps(out, ensure_ascii=False, indent=2))
            return 0
        names = (list(chain_scenarios.SCENARIOS) if args.scenario == "all"
                 else [args.scenario])
        if args.export:
            for name in names:
                path = f"{args.export.rstrip('/')}/{name}.json"
                chain_scenarios.dump(name, path)
                print(f"[export] {name} -> {path}")
            return 0
        out = {}
        for name in names:
            out[name] = run_chain_scenario(c, name)
        print(json.dumps(out, ensure_ascii=False, indent=2))
    elif args.cmd == "gaps":
        q = []
        if args.cargo_batch_id:
            q.append(f"cargo_batch_id={args.cargo_batch_id}")
        if args.device:
            q.append(f"device_id={args.device}")
        if args.active:
            q.append("active_only=true")
        data = c.chain_gaps("?" + "&".join(q) if q else "")
        for g in data["gaps"]:
            print(f"#{g['id']} {g['device_id']}/boot-{g['boot_batch']} "
                  f"seq[{g['seq_from']},{g['seq_to']}] {g['status']} "
                  f"{g['cause_label']} @ {g['cargo_batch_id']} "
                  f"v{g['version']} {g['time_from']}~{g['time_to']}")
            for s in g.get("affected_segments", []):
                print(f"    区段 {s['cargo_batch_id']}/{s['box_id']} "
                      f"{s['coverage']} {s['observed_count']}/{s['expected_count']} "
                      f"{s['effective_from']}~{s['effective_to']}")
    elif args.cmd == "backfill":
        data = chain_scenarios.build(args.scenario)
        bf = data.get("backfill")
        if not bf:
            print(f"[backfill] 场景 {args.scenario} 没有补传数据")
            return 1
        print(json.dumps(c.chain_backfill(bf["device_id"], bf["boot_batch"],
                                          bf["events"]), ensure_ascii=False,
                         indent=2))
    elif args.cmd == "consume":
        cons = c.create_consumer(args.name, args.purpose)
        cid = cons["id"]
        total = 0
        while True:
            page = c.pull(cid, args.size)
            items = page.get("items", [])
            if not items:
                print(f"[{args.name}] 无更多数据：{page.get('note', '')}")
                break
            print(f"[{args.name}] 版本 {page['version']} 分页 {page['from']}..{page['to']}"
                  f"{'（重发）' if page.get('redelivered') else ''}：{len(items)} 条")
            for it in items:
                print(f"  seq={it['seq']:>3} {it['device_id']:<9} {it['std_c']:>7}°C"
                      f"  t={it['sample_time']} ±{it['time_error_s']:.1f}s"
                      f"  {it.get('vehicle_id')}/{it.get('cargo_batch_id')}")
            c.ack(cid, page["version"], page["from"], page["to"])
            total += len(items)
            if not page.get("has_more"):
                break
        print(f"[{args.name}] 共接收 {total} 条，游标推进完成")


def run_chain_bundle(c: Client, data: dict, reset: bool = True) -> dict:
    """导入导出文件（或脚本给出的同一结构）并返回诊断摘要。"""
    if reset:
        c.reset()
        c.seed()
    c.chain_import({"relations": data.get("relations") or [],
                    "plans": data.get("plans") or [],
                    "tasks": data.get("tasks") or [],
                    "manifests": data.get("manifests") or []})
    for ev in data.get("ingest") or []:
        c.chain_ingest(ev["device_id"], ev["boot_batch"], ev["events"])
    refs = c.chain_refresh()
    gaps = c.chain_gaps("?active_only=true")
    return {"refresh": [{"scope": r["scope_key"], "gaps": r["gaps"],
                         "created": r["created"], "filled": r["filled"]}
                        for r in refs["refreshes"]],
            "gaps": [{"id": g["id"], "seq": [g["seq_from"], g["seq_to"]],
                      "status": g["status"], "cause": g["cause"],
                      "segment": g["segment"], "version": g["version"]}
                     for g in gaps["gaps"]]}


def run_chain_scenario(c: Client, name: str, reset: bool = True) -> dict:
    """独立演练一个场景：重置→导入计划/任务/清单→平台接入→刷新→返回缺口摘要。"""
    data = chain_scenarios.build(name)
    if reset:
        c.reset()
        c.seed()
    imp = c.chain_import({"relations": data.get("relations") or [],
                          "plans": data["plans"], "tasks": data["tasks"],
                          "manifests": data["manifests"]})
    for ev in data.get("ingest") or []:
        c.chain_ingest(ev["device_id"], ev["boot_batch"], ev["events"])
    refreshes = c.chain_refresh()
    gaps = c.chain_gaps("?active_only=true")
    summary = {
        "import": imp,
        "refresh": [{"scope": r["scope_key"], "gaps": r["gaps"],
                     "created": r["created"], "changed": r["changed"],
                     "filled": r["filled"]} for r in refreshes["refreshes"]],
        "gaps": [{"id": g["id"], "device_id": g["device_id"],
                  "boot_batch": g["boot_batch"],
                  "seq": [g["seq_from"], g["seq_to"]],
                  "status": g["status"], "cause": g["cause"],
                  "cause_label": g["cause_label"], "segment": g["segment"],
                  "version": g["version"],
                  "affected_segments": [
                      {"cargo_batch_id": s["cargo_batch_id"],
                       "box_id": s["box_id"], "coverage": s["coverage"],
                       "observed": s["observed_count"],
                       "expected": s["expected_count"]}
                      for s in g.get("affected_segments", [])]}
                 for g in gaps["gaps"]],
    }
    if data.get("backfill"):
        bf = data["backfill"]
        summary["backfill"] = c.chain_backfill(bf["device_id"], bf["boot_batch"],
                                               bf["events"])
        after = c.chain_gaps(f"?device_id={bf['device_id']}")
        summary["after_backfill"] = [
            {"id": g["id"], "status": g["status"],
             "seq": [g["seq_from"], g["seq_to"]],
             "affected_segments": [
                 {"cargo_batch_id": s["cargo_batch_id"],
                  "coverage": s["coverage"],
                  "observed": s["observed_count"],
                  "expected": s["expected_count"],
                  "remaining_gap": s["remaining_gap"]}
                 for s in g.get("affected_segments", [])]}
            for g in after["gaps"]]
    return summary


if __name__ == "__main__":
    sys.exit(main())
