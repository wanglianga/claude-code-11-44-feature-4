#!/usr/bin/env python3
"""本地发送器 CLI：按可控顺序发送两类协议报文。

用法：
  python sender.py reset                     # 清空服务端数据
  python sender.py seed                      # 写入种子契约/关系/锚点
  python sender.py run <场景名|all>          # 按顺序执行场景
  python sender.py publish                   # 构建两个用途的发布版本
  python sender.py consume <name> <purpose>  # 模拟消费端按游标读完整版本

场景：binary-main | binary-restart | json-basic | json-new-firmware | contract-revision
"""
import argparse
import json
import sys

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


if __name__ == "__main__":
    sys.exit(main())
