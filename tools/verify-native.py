#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""verify-native.py —— 用仓库自带 pipeline 校验节点存活（不自己造轮子）

它做的就是 subscribe/pipeline.py: check_alive_proxies()：
  · clash.generate_config()  生成测试配置
  · 起 mihomo 内核
  · clash.check()            逐个节点打多目标（test_url + YouTube 资源），全部通过才算活

用法:
  python tools/verify-native.py -f proxies.yaml                    # 只校验
  python tools/verify-native.py -f proxies.yaml -o best-nodes.yaml # 校验并写清单
  python tools/verify-native.py -f proxies.yaml --core "C:/Program Files/Clash Verge/verge-mihomo.exe"

注意:
  - 需要先 pip install -r requirements.txt（geoip2 / pycryptodomex / tqdm / PyYAML）
  - 本机若开着 TUN，测出来的"活"是经现有代理中转的假结果，脚本会先拦一道
  - 仓库 clash/ 自带的 clash 不支持 vless/reality/hysteria2，建议用 --core 指向 mihomo
"""
import argparse
import os
import subprocess
import sys
import tempfile

import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, 'subscribe'))

DEFAULT_CORES = [
    r'C:\Program Files\Clash Verge\verge-mihomo.exe',
    os.path.join(ROOT, 'clash', 'clash-windows-amd.exe'),
    os.path.join(ROOT, 'clash', 'clash-linux-amd'),
]


def warn_tun():
    try:
        out = subprocess.run(['ipconfig'], capture_output=True, text=True,
                             errors='ignore', timeout=20).stdout
        if '198.18.0.1' in out:
            print('!' * 68)
            print('! 检测到 TUN 虚拟网卡(198.18.x.x)：流量被接管，校验结果不可信。')
            print('! 请先关闭 TUN 模式再跑。')
            print('!' * 68)
            return False
    except Exception:
        pass
    return True


def pick_core(explicit):
    if explicit:
        return explicit
    for c in DEFAULT_CORES:
        if c and os.path.exists(c):
            return c
    sys.exit('找不到可用内核，请用 --core 指定')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('-f', '--file', required=True, help='含 proxies 的 yaml')
    ap.add_argument('-o', '--out', help='把通过校验的节点写成新的 yaml')
    ap.add_argument('--core', help='mihomo/clash 内核路径')
    ap.add_argument('--url', default='https://www.gstatic.com/generate_204', help='test_url')
    ap.add_argument('--delay', type=int, default=12000, help='延迟上限 ms')
    ap.add_argument('--timeout', type=int, default=12000, help='单次请求超时 ms')
    ap.add_argument('--threads', type=int, default=8)
    ap.add_argument('--force', action='store_true')
    args = ap.parse_args()

    if not warn_tun() and not args.force:
        sys.exit('已中止（加 --force 可强行继续）')

    import pipeline  # 仓库自带模块

    raw = yaml.safe_load(open(args.file, encoding='utf-8'))
    proxies = raw['proxies'] if isinstance(raw, dict) else raw
    print(f'待校验: {len(proxies)} 个节点    内核: {pick_core(args.core)}')

    ws = tempfile.mkdtemp(prefix='native-verify-')
    alive = pipeline.check_alive_proxies(
        proxies=proxies,
        clash_bin=pick_core(args.core),
        workspace=ws,
        filename='config.yaml',
        timeout=args.timeout,
        test_url=args.url,
        delay=args.delay,
        num_threads=args.threads,
        display=False,
    )
    names = {p['name'] for p in proxies}
    ok = [p for p in alive if p.get('name') in names]
    print(f'\n通过: {len(ok)}/{len(proxies)}')
    for p in ok:
        print(f"  ✓ {p.get('type'):9s} {p.get('server')}:{p.get('port')}  {p.get('name')}")
    dead = sorted(names - {p['name'] for p in ok})
    if dead:
        print('未通过:', ', '.join(dead))

    if args.out:
        yaml.safe_dump({'proxies': ok}, open(args.out, 'w', encoding='utf-8'),
                       allow_unicode=True, sort_keys=False)
        print('已写入', os.path.abspath(args.out))


if __name__ == '__main__':
    main()
