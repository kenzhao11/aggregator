#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""pool-build.py —— 多源聚合，解析交给仓库自带的 AirPort.decode()

不自己实现协议解析：clash yaml 走 AirPort.decode(use_subconverter=False)，
链接/base64 订阅走 AirPort.decode(use_subconverter=True)（内部调用 subconverter 二进制）。

用法:
  python tools/pool-build.py -s subscribes.txt -o pool.yaml
  python tools/pool-build.py -o pool.yaml --cap 500

前置:
  1) pip install -r requirements.txt
  2) subconverter/ 下要有对应平台的二进制（Windows: subconverter-windows-amd.exe）
"""
import argparse
import os
import sys
import time

import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, 'subscribe'))

DEFAULT_SOURCES = os.path.join(ROOT, 'subscribes.txt')


def load_sources(path):
    urls = []
    for line in open(path, encoding='utf-8'):
        line = line.strip()
        if line and not line.startswith('#'):
            urls.append(line)
    return urls


def looks_like_clash(text):
    head = text.lstrip()[:400]
    return '"proxies"' in head or head.startswith('proxies:') or 'proxy-groups' in head


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('-s', '--sources', default=DEFAULT_SOURCES)
    ap.add_argument('-o', '--out', default='pool.yaml')
    ap.add_argument('--cap', type=int, default=0, help='最多保留多少节点')
    args = ap.parse_args()

    import executable
    import utils
    from airport import AirPort

    clash_bin, sub_bin = executable.which_bin()
    urls = load_sources(args.sources)
    print(f'源: {len(urls)} 个    解析器: AirPort.decode（自带）    转换器: {sub_bin}')

    pool, seen, stats = [], set(), []
    for idx, url in enumerate(urls, 1):
        t0 = time.time()
        text = utils.http_get(url=url, retry=2, timeout=120)
        if not text:
            stats.append((url, 0, 'fetch-fail'))
            print(f'  [{idx}] 抓取失败  {url}')
            continue
        use_sub = not looks_like_clash(text)
        try:
            got = AirPort.decode(text=text, program=clash_bin, artifact=f'pool{idx}',
                                 use_subconverter=use_sub, do_verify=False)
        except Exception as e:
            got = []
            print(f'  [{idx}] 解析异常 {type(e).__name__}: {str(e)[:60]}')
        added = 0
        for p in got or []:
            if not isinstance(p, dict) or not p.get('server') or not p.get('port'):
                continue
            key = (str(p['server']).lower(), int(p['port']), p.get('type'),
                   str(p.get('uuid') or p.get('password') or '')[:16])
            if key in seen:
                continue
            seen.add(key)
            p['name'] = f"{p.get('name') or p['server']}#{len(pool)}"
            pool.append(p)
            added += 1
        stats.append((url, added, 'subconverter' if use_sub else 'yaml'))
        print(f'  [{idx}] +{added:5d}  {("subconverter" if use_sub else "yaml"):13s} {round(time.time()-t0,1)}s  {url.split("/")[3] if url.count("/") > 3 else url}')

    if args.cap:
        pool = pool[:args.cap]

    from collections import Counter
    print(f'\n去重后 {len(pool)} 个节点: {dict(Counter(p["type"] for p in pool))}')
    cfg = {'mixed-port': 7890, 'allow-lan': False, 'mode': 'rule', 'log-level': 'info',
           'proxies': pool,
           'proxy-groups': [{'name': 'PROBE', 'type': 'select',
                             'proxies': [p['name'] for p in pool] + ['DIRECT']}],
           'rules': ['MATCH,PROBE']}
    with open(args.out, 'w', encoding='utf-8') as f:
        yaml.safe_dump(cfg, f, allow_unicode=True, sort_keys=False)
    print('已写入', os.path.abspath(args.out))
    print('下一步: python tools/verify-native.py -f', args.out)


if __name__ == '__main__':
    main()
