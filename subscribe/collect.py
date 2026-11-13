# -*- coding: utf-8 -*-

# @Author  : wzdnzd
# @Time    : 2022-07-15

import argparse
from functools import partial
import itertools
import os
import random
import re
import shutil
import subprocess
import sys
import time

import executable
import pipeline
import push
import utils
import workflow
import yaml
from airport import AirPort
from config.models import NodeInput, SiteConfig, StorageItem
from crawl.helpers import check_status, naming_task
from discovery import AirportRecord, collect_airport, parse_records, save_records
from logger import logger
from urlvalidator import isurl
from workflow import TaskConfig

import clash
import location
import subconverter

PATH = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))

DATA_BASE = os.path.join(PATH, "data")


def assign(
    bin_name: str,
    domains_file: str = "",
    overwrite: bool = False,
    pages: int = sys.maxsize,
    allow_gmail_alias: bool = False,
    display: bool = True,
    num_threads: int = 0,
    **kwargs: object,
) -> list[TaskConfig]:
    def load_exist(username: str, gist_id: str, access_token: str, filename: str) -> list[str]:
        if not filename:
            return []

        subscriptions = set()

        pattern = r"^https?:\/\/[^\s]+"
        local_file = os.path.join(DATA_BASE, filename)
        if os.path.exists(local_file) and os.path.isfile(local_file):
            with open(local_file, "r", encoding="utf8") as f:
                items = re.findall(pattern, str(f.read()), flags=re.M)
                if items:
                    subscriptions.update(items)

        if username and gist_id and access_token:
            push_tool = push.PushToGist(token=access_token)
            url = push_tool.raw_url(item=StorageItem(username=username, gist_id=gist_id, filename=filename))

            content = utils.http_get(url=url, timeout=30)
            items = re.findall(pattern, content, flags=re.M)
            if items:
                subscriptions.update(items)

        logger.info("start checking whether existing subscriptions have expired")

        # 过滤已过期订阅并返回
        links = list(subscriptions)
        results = utils.multi_thread_run(
            func=check_status,
            tasks=links,
            num_threads=num_threads,
            show_progress=display,
        )

        return [links[i] for i in range(len(links)) if results[i][0] and not results[i][1]]

    subscribes_file = utils.trim(kwargs.get("subscribes_file", ""))
    access_token = utils.trim(kwargs.get("access_token", ""))
    gist_id = utils.trim(kwargs.get("gist_id", ""))
    username = utils.trim(kwargs.get("username", ""))
    skip_captcha = kwargs.get("skip_captcha", False)

    # 加载已有订阅
    subscriptions = load_exist(username, gist_id, access_token, subscribes_file)
    logger.info(f"load exists subscription finished, count: {len(subscriptions)}")

    # 是否允许特殊协议
    special_protocols = AirPort.enable_special_protocols()

    tasks = (
        [
            TaskConfig(
                name=utils.random_chars(length=8),
                nodes=NodeInput(subscribe=x),
                bin_name=bin_name,
                special_protocols=special_protocols,
            )
            for x in subscriptions
            if x
        ]
        if subscriptions
        else []
    )

    # 仅更新已有订阅
    if tasks and kwargs.get("refresh", False):
        logger.info("skip registering new accounts, will use existing subscriptions for refreshing")
        return tasks

    records: dict[str, AirportRecord] = {}
    delimiter = "@#@#"
    domains_file = utils.trim(domains_file)
    if not domains_file:
        domains_file = "domains.txt"

    # 加载已有站点列表
    fullpath = os.path.join(DATA_BASE, domains_file)
    if os.path.exists(fullpath) and os.path.isfile(fullpath):
        with open(fullpath, "r", encoding="UTF8") as handle:
            records.update(parse_records(content=str(handle.read()), delimiter=delimiter))

    # 爬取新站点列表
    if not records or overwrite:
        crawled = collect_airport(
            channel="jichang_list",
            page_num=pages,
            num_threads=num_threads,
            allow_gmail_alias=allow_gmail_alias,
            display=display,
            filepath=os.path.join(DATA_BASE, "coupons.txt"),
            delimiter=delimiter,
            skip_captcha=skip_captcha,
        )

        if crawled:
            for domain, incoming in crawled.items():
                current = records.get(domain)
                if current is None:
                    records[domain] = incoming
                else:
                    current.coupon = incoming.coupon
                    current.api_prefix = incoming.api_prefix
            overwrite = True

    # 加载自定义机场列表
    customize_link = utils.trim(kwargs.get("customize_link", ""))
    if customize_link:
        if isurl(customize_link):
            records.update(parse_records(content=utils.http_get(url=customize_link), delimiter=delimiter))
        else:
            local_file = os.path.join(DATA_BASE, customize_link)
            if local_file != fullpath and os.path.exists(local_file) and os.path.isfile(local_file):
                with open(local_file, "r", encoding="UTF8") as handle:
                    records.update(parse_records(content=str(handle.read()), delimiter=delimiter))

    if not records:
        logger.error("cannot collect any new airport for free use")
        return tasks

    if overwrite:
        save_records(records=records, filepath=fullpath, delimiter=delimiter, full=True)

    sites = []
    for domain, record in records.items():
        name = naming_task(url=domain)
        sites.append(
            SiteConfig(
                name=name,
                domain=domain,
                coupon=record.coupon,
                invite_code=record.invite_code,
                api_prefix=record.api_prefix or "/api/v1/",
                skip_captcha=skip_captcha,
            )
        )

    assigned, _ = pipeline.assign_sites(
        sites=sites,
        groups={},
        retry=3,
        bin_name=bin_name,
        allow_gmail_alias=allow_gmail_alias,
    )
    tasks.extend(assigned)
    return tasks


# --- Region-based trim (kenzhao11 fork): keep the subscription small ---
# Cap the generated Clash/V2Ray/Sing-box output to ~50-70 nodes across 6
# regions (US weighted heaviest, per Ken 2026-11-13), so the subscription
# does not balloon to thousands of nodes. Nodes are classified by region
# from their name first, then from their egress IP country (via the GeoIP
# mmdb in location.py). Unknown-region nodes only fill the total up to the
# cap. Fail-safe: on any error the untrimmed list is returned unchanged.
# Ken 2026-11-13: US first, then JP/SG/KR/TW/HK. EU/MY dropped.
TRIM_TARGETS = {"US": 20, "JP": 12, "SG": 12, "HK": 10, "TW": 6, "KR": 6}

REGION_PATTERNS = {
    "HK": ["香港", "🇭🇰", "Hong Kong", "HongKong"],
    "TW": ["台湾", "台灣", "🇹🇼", "Taiwan"],
    "JP": ["日本", "🇯🇵", "Japan", "东京", "東京", "大阪", "Tokyo", "Osaka"],
    "KR": ["韩国", "韓國", "🇰🇷", "Korea", "首尔", "Seoul"],
    "US": ["美国", "🇺🇸", "USA", "America", "United States", "洛杉矶", "Los Angeles", "纽约", "New York", "旧金山", "San Francisco", "西雅图", "Seattle", "芝加哥", "Chicago"],
    "DE": ["德国", "🇩🇪", "Germany", "法兰克福", "Frankfurt", "Berlin", "柏林"],
    "UK": ["英国", "🇬🇧", "England", "United Kingdom", "伦敦", "London"],
    "FR": ["法国", "🇫🇷", "France", "巴黎", "Paris"],
    "SG": ["新加坡", "🇸🇬", "Singapore"],
    "MY": ["马来西亚", "🇲🇾", "Malaysia", "吉隆坡", "Kuala Lumpur"],
}

_PROTO_RANK = {"vless": 0, "trojan": 1, "ss": 2, "vmess": 3, "hysteria2": 4, "hysteria": 5}


def _region_from_name(name: str) -> str:
    n = str(name or "")
    for code, pats in REGION_PATTERNS.items():
        for p in pats:
            if p in n:
                return code
    return ""


# Chinese country name (from the MaxMind GeoIP mmdb) -> region code, for
# egress-IP based classification. CDN egress IPs are never a country
# (see location.lookup_ip_geo), so Cloudflare-fronted nodes stay unknown
# instead of being mislabelled as US.
_COUNTRY_TO_REGION = {
    "美国": "US",
    "日本": "JP",
    "新加坡": "SG",
    "香港": "HK",
    "台湾": "TW",
    "韩国": "KR",
    "德国": "DE",
    "英国": "UK",
    "法国": "FR",
    "马来西亚": "MY",
}


def _region_from_egress_ip(node: dict, reader) -> str:
    """Classify a node by its egress IP country (set by the verify step)."""
    ip = str(node.get("egress_ip", "") or "")
    if not ip or reader is None:
        return ""
    try:
        geo = location.lookup_ip_geo(ip, reader)
        if geo.is_cdn or not geo.country:
            return ""
        return _COUNTRY_TO_REGION.get(geo.country, "")
    except Exception as e:
        logger.warning(f"egress-ip region lookup failed ({e})")
        return ""


def _check_reachable(proxy: dict, port: int) -> "location.ProxyQueryResult":
    """Real-traffic check: fetch a real web page through this proxy.

    A node only counts as usable if it can fetch a real page (not just a
    10-byte IP echo) fast enough to be useful, and return its egress IP.
    The old IP-echo-only check was too easy: a node could pass it while
    being far too slow or broken for real browsing.
    """
    name = str(proxy.get("name", ""))
    result = location.ProxyInfo(name=name)
    if not port:
        return location.ProxyQueryResult(proxy=proxy, result=result, success=False)
    try:
        started = time.time()
        ok, body = location.make_proxy_request(
            port,
            "https://www.google.com/",
            max_retries=1,
            timeout=8,
            deserialize=False,
            quiet=True,
        )
        fetch_ms = int((time.time() - started) * 1000)
        if not ok or not body or len(body) < 1024:
            return location.ProxyQueryResult(proxy=proxy, result=result, success=False)
        proxy["fetch_ms"] = fetch_ms
        ip = location.resolve_egress_ipv4(
            port,
            partial(location.make_proxy_request, quiet=True),
            max_retries=1,
            timeout=10,
        )
        if not ip:
            return location.ProxyQueryResult(proxy=proxy, result=result, success=False)
        proxy["egress_ip"] = ip
        return location.ProxyQueryResult(proxy=proxy, result=result, success=True)
    except Exception as e:
        logger.warning(f"real-traffic check failed for proxy {name}: {e}")
        return location.ProxyQueryResult(proxy=proxy, result=result, success=False)


def verify_reachable_proxies(nodes: list, num_threads: int = 0) -> list:
    """Drop every proxy that cannot really fetch traffic (fail-safe).

    Starts one mihomo instance with a dedicated HTTP listener per proxy
    and fetches the egress IP through each of them. Only proxies that
    return a real egress IP are kept.
    """
    if not nodes:
        return nodes
    if not location.is_mihomo():
        logger.warning("mihomo binary not found, skip real-traffic check")
        return nodes
    try:
        results = location.batch_query(
            proxies=nodes,
            func=_check_reachable,
            num_threads=num_threads or 32,
            show_progress=False,
            description="Verifying real traffic",
        )
        kept = [r.proxy for r in results if r and r.success]
        logger.info(f"real-traffic check: {len(kept)}/{len(nodes)} proxies can really fetch traffic")
        if not kept:
            logger.warning("real-traffic check kept 0 proxies, keep clash-checked nodes instead")
            return nodes
        return kept
    except Exception as e:
        logger.error(f"real-traffic check failed ({e}), keep clash-checked nodes")
        return nodes


def trim_proxies_by_region(nodes: list, targets: dict = None) -> list:
    try:
        targets = targets or TRIM_TARGETS
        total_cap = sum(targets.values())
        reader = None
        if any(isinstance(nd, dict) and nd.get("egress_ip") for nd in nodes):
            try:
                os.makedirs(DATA_BASE, exist_ok=True)
                reader = location.load_mmdb(directory=DATA_BASE, update=False)
            except Exception as e:
                logger.warning(f"cannot load geoip mmdb ({e})")
        by_region = {}
        unknown = []
        for nd in nodes:
            r = ""
            if isinstance(nd, dict):
                # Egress IP is ground truth for where traffic exits; the name
                # is only a label the source typed (often wrong). Fall back
                # to the name when there is no egress IP (unverified nodes).
                r = _region_from_egress_ip(nd, reader)
                if not r:
                    r = _region_from_name(nd.get("name", ""))
                nd["_region"] = r
            if r in targets:
                by_region.setdefault(r, []).append(nd)
            else:
                unknown.append(nd)

        def _rank(nd):
            # Prefer TLS on 443: it blends with normal HTTPS traffic and
            # survives the GFW far better than random high ports. Then the
            # fastest real page fetch, then handshake delay.
            tls443 = 2
            if isinstance(nd, dict):
                if nd.get("tls") and str(nd.get("port", "")) == "443":
                    tls443 = 0
                elif nd.get("tls"):
                    tls443 = 1
            fetch = nd.get("fetch_ms") if isinstance(nd, dict) else None
            has_fetch = 0 if isinstance(fetch, (int, float)) else 1
            f = float(fetch) if isinstance(fetch, (int, float)) else 0.0
            delay = nd.get("delay") if isinstance(nd, dict) else None
            has_delay = 0 if isinstance(delay, (int, float)) else 1
            d = float(delay) if isinstance(delay, (int, float)) else 0.0
            proto = str(nd.get("type", "")).lower() if isinstance(nd, dict) else ""
            return (tls443, has_fetch, f, has_delay, d, _PROTO_RANK.get(proto, 9))

        kept = []
        for code, want in targets.items():
            cands = sorted(by_region.get(code, []), key=_rank)
            kept.extend(cands[:want])
        if len(kept) < total_cap and unknown:
            unknown_sorted = sorted(unknown, key=_rank)
            kept.extend(unknown_sorted[: total_cap - len(kept)])
        seen = set()
        out = []
        for nd in kept:
            key = (str(nd.get("server", "")), str(nd.get("port", "")))
            if key in seen:
                continue
            seen.add(key)
            out.append(nd)
        # Label kept nodes with their real region so the subscription shows
        # which node is US/JP/SG/... instead of generic VLESS-xxx names.
        region_label = {
            "US": "🇺🇸 美国",
            "JP": "🇯🇵 日本",
            "SG": "🇸🇬 新加坡",
            "HK": "🇭🇰 香港",
            "TW": "🇹🇼 台湾",
            "KR": "🇰🇷 韩国",
            "DE": "🇩🇪 德国",
            "UK": "🇬🇧 英国",
            "FR": "🇫🇷 法国",
            "MY": "🇲🇾 马来西亚",
        }
        counts = {}
        for nd in out:
            r = nd.pop("_region", "") if isinstance(nd, dict) else ""
            if isinstance(nd, dict):
                nd.pop("egress_ip", None)
                nd.pop("fetch_ms", None)
            if r in region_label:
                counts[r] = counts.get(r, 0) + 1
                nd["name"] = f"{region_label[r]} {counts[r]:02d}"
        breakdown = ", ".join(f"{k}={v}" for k, v in sorted(counts.items()))
        logger.info(
            f"trim proxies by region: {len(nodes)} -> {len(out)}"
            + (f" [{breakdown}]" if breakdown else "")
            + f", unknown={len(out) - sum(counts.values())}"
        )
        return out
    except Exception as e:
        logger.error(f"trim proxies by region failed ({e}), keep untrimmed proxies")
        return nodes


def aggregate(args: argparse.Namespace) -> None:
    def parse_gist_link(link: str) -> tuple[str, str]:
        # 提取 gist 用户名及 id
        words = utils.trim(link).split("/", maxsplit=1)
        if len(words) != 2:
            logger.error(f"cannot extract username and gist id due to invalid github gist link")
            return "", ""

        return utils.trim(words[0]), utils.trim(words[1])

    clash_bin, subconverter_bin = executable.which_bin()
    display = not args.invisible

    subscribes_file = "subscribes.txt"
    access_token = utils.trim(args.key)
    username, gist_id = parse_gist_link(args.gist)

    tasks = assign(
        bin_name=subconverter_bin,
        domains_file="domains.txt",
        overwrite=args.overwrite,
        pages=args.pages,
        allow_gmail_alias=args.easygoing,
        display=display,
        num_threads=args.num,
        refresh=args.refresh,
        skip_captcha=args.skip_captcha,
        username=username,
        gist_id=gist_id,
        access_token=access_token,
        subscribes_file=subscribes_file,
        customize_link=args.custom_sites,
    )

    if not tasks:
        logger.error("cannot found any valid config, exit")
        sys.exit(0)

    # 已有订阅已经做过过期检查，无需再测
    old_subscriptions = set()
    for task in tasks:
        old_subscriptions.update(task.nodes.subscribe_list())

    logger.info(f"start generate subscribes information, tasks: {len(tasks)}")
    generate_conf = os.path.join(PATH, "subconverter", "generate.ini")
    if os.path.exists(generate_conf) and os.path.isfile(generate_conf):
        os.remove(generate_conf)

    results = pipeline.execute_tasks(tasks)
    proxies = list(itertools.chain.from_iterable([x[1] for x in results if x]))

    if len(proxies) == 0:
        logger.error("exit because cannot fetch any proxy node")
        sys.exit(0)

    nodes, workspace = [], os.path.join(PATH, "clash")

    if args.skip:
        nodes = clash.filter_proxies(proxies).get("proxies", [])
    else:
        binpath = os.path.join(workspace, clash_bin)
        confif_file = "config.yaml"
        proxies = clash.generate_config(workspace, list(proxies), confif_file)

        # 可执行权限
        utils.chmod(binpath)

        logger.info(f"startup clash now, workspace: {workspace}, config: {confif_file}")
        process = subprocess.Popen(
            [
                binpath,
                "-d",
                workspace,
                "-f",
                os.path.join(workspace, confif_file),
            ]
        )
        logger.info(f"clash start success, begin check proxies, num: {len(proxies)}")

        time.sleep(random.randint(3, 6))
        params = [
            [p, clash.EXTERNAL_CONTROLLER, 5000, args.url, args.delay, False] for p in proxies if isinstance(p, dict)
        ]

        masks = utils.multi_thread_run(
            func=clash.check,
            tasks=params,
            num_threads=args.num,
            show_progress=display,
        )

        # 关闭clash
        try:
            process.terminate()
        except:
            logger.error(f"terminate clash process error")

        nodes = [proxies[i] for i in range(len(proxies)) if masks[i]]
        if len(nodes) <= 0:
            logger.error(f"cannot fetch any proxy")
            sys.exit(0)

    # Ken 2026-11-13: only keep nodes that can REALLY fetch traffic.
    # The clash delay check only proves a handshake; this step fetches the
    # egress IP through every surviving proxy and drops the fakes.
    # Skip-mode (weekly Collect) has no delay filter, so it stays unverified.
    if not args.skip:
        nodes = verify_reachable_proxies(nodes, num_threads=args.num)

    # Trim to ~50-70 nodes across 6 regions (fail-safe inside).
    nodes = trim_proxies_by_region(nodes)

    subscriptions = set()
    for p in proxies:
        # 移除无用的标记
        p.pop("liveness", True)

        sub = p.pop("sub", "")
        if sub:
            subscriptions.add(sub)

    data = {"proxies": nodes}
    urls = list(subscriptions)
    source = "proxies.yaml"

    # 如果文件夹不存在则创建
    os.makedirs(DATA_BASE, exist_ok=True)

    supplier = os.path.join(PATH, "subconverter", source)
    if os.path.exists(supplier) and os.path.isfile(supplier):
        os.remove(supplier)

    with open(supplier, "w+", encoding="utf8") as f:
        yaml.add_representer(clash.QuotedStr, clash.quoted_scalar)
        yaml.dump(data, f, allow_unicode=True)

    if os.path.exists(generate_conf) and os.path.isfile(generate_conf):
        os.remove(generate_conf)

    targets, records = [], {}
    for target in args.targets:
        target = utils.trim(target).lower()
        convert_name = f'convert_{target.replace("&", "_").replace("=", "_")}'

        filename = subconverter.get_filename(target=target)
        list_only = False if target == "v2ray" or target == "mixed" or "ss" in target else not args.all
        targets.append((convert_name, filename, target, list_only, args.ignore_default_filters))

    for t in targets:
        success = subconverter.generate_conf(generate_conf, t[0], source, t[1], t[2], True, t[3], t[4])
        if not success:
            logger.error(f"cannot generate subconverter config file for target: {t[2]}")
            continue

        if subconverter.convert(binname=subconverter_bin, artifact=t[0]):
            filepath = os.path.join(DATA_BASE, t[1])
            shutil.move(os.path.join(PATH, "subconverter", t[1]), filepath)

            records[t[1]] = filepath

    if len(records) > 0:
        os.remove(supplier)
    else:
        logger.error(f"all targets convert failed, you can view the temporary file: {supplier}")
        sys.exit(1)

    logger.info(f"found {len(nodes)} proxies, save it to {list(records.values())}")

    life, traffic = max(0, args.life), max(0, args.flow)
    if life > 0 or traffic > 0:
        # 过滤出新的订阅并检查剩余流量和过期时间是否满足要求
        new_subscriptions = [x for x in urls if x not in old_subscriptions]

        tasks = [[x, 2, traffic, life, 0, True] for x in new_subscriptions]
        results = utils.multi_thread_run(
            func=check_status,
            tasks=tasks,
            num_threads=args.num,
            show_progress=display,
        )

        total = len(urls)

        # 筛选出为符合要求的订阅
        urls = [new_subscriptions[i] for i in range(len(new_subscriptions)) if results[i][0] and not results[i][1]]
        discard = len(tasks) - len(urls)

        # 合并新老订阅
        urls.extend(list(old_subscriptions))

        logger.info(f"filter subscriptions finished, total: {total}, found: {len(urls)}, discard: {discard}")

    utils.write_file(filename=os.path.join(DATA_BASE, subscribes_file), lines=urls)
    domains = [utils.extract_domain(url=x, include_protocal=True) for x in urls]

    # 保存实际可使用的网站列表
    utils.write_file(filename=os.path.join(DATA_BASE, "valid-domains.txt"), lines=list(set(domains)))

    # 如有必要，上传至 Gist
    if gist_id and access_token:
        files, item = {}, StorageItem(gist_id=gist_id, filename=list(records.keys())[0])

        for k, v in records.items():
            if os.path.exists(v) and os.path.isfile(v):
                with open(v, "r", encoding="utf8") as f:
                    lines = utils.trim(f.read())
                    if lines:
                        files[k] = {"content": lines, "filename": k}

        if urls:
            files[subscribes_file] = {"content": "\n".join(urls), "filename": subscribes_file}

        if files:
            push_client = push.PushToGist(token=access_token)

            # 上传
            success = push_client.push_to(content="", item=item, payload={"files": files}, group="collect")
            if success:
                logger.info(f"upload proxies and subscriptions to gist successed")
            else:
                logger.error(f"upload proxies and subscriptions to gist failed")

    # 清理工作空间
    workflow.cleanup(workspace, [])


class CustomHelpFormatter(argparse.HelpFormatter):
    def _format_action_invocation(self, action: argparse.Action) -> str:
        if action.choices:
            parts = []
            if action.option_strings:
                parts.extend(action.option_strings)

                # 移除使用帮助信息中 -t 或 --targets 附带的过长的可选项信息
                if action.nargs != 0 and action.option_strings != ["-t", "--targets"]:
                    default = action.dest.upper()
                    args_string = self._format_args(action, default)
                    parts[-1] += " " + args_string
            else:
                args_string = self._format_args(action, action.dest)
                parts.append(args_string)
            return ", ".join(parts)
        else:
            return super()._format_action_invocation(action)


if __name__ == "__main__":
    env_parser = argparse.ArgumentParser(add_help=False)
    env_parser.add_argument(
        "--environment",
        type=str,
        default=".env",
        help="environment file name",
    )
    env_args, _ = env_parser.parse_known_args()
    utils.load_dotenv(env_args.environment)

    parser = argparse.ArgumentParser(formatter_class=CustomHelpFormatter, parents=[env_parser])
    parser.add_argument(
        "-a",
        "--all",
        dest="all",
        action="store_true",
        default=False,
        help="generate full configuration for clash",
    )

    parser.add_argument(
        "-c",
        "--skip-captcha",
        dest="skip_captcha",
        action="store_true",
        default=False,
        help="discard candidate sites that may require human-authentication",
    )

    parser.add_argument(
        "-d",
        "--delay",
        type=int,
        required=False,
        default=5000,
        help="proxies max delay allowed",
    )

    parser.add_argument(
        "-e",
        "--easygoing",
        dest="easygoing",
        action="store_true",
        default=False,
        help="try registering with a gmail alias when you encounter a whitelisted mailbox",
    )

    parser.add_argument(
        "-f",
        "--flow",
        type=int,
        required=False,
        default=0,
        help="remaining traffic available for use, unit: GB",
    )

    parser.add_argument(
        "-g",
        "--gist",
        type=str,
        required=False,
        default=os.environ.get("GIST_LINK", ""),
        help="github username and gist id, separated by '/'",
    )

    parser.add_argument(
        "-i",
        "--invisible",
        dest="invisible",
        action="store_true",
        default=False,
        help="don't show check progress bar",
    )

    parser.add_argument(
        "-k",
        "--key",
        type=str,
        required=False,
        default=os.environ.get("GIST_PAT", ""),
        help="github personal access token for editing gist",
    )

    parser.add_argument(
        "-l",
        "--life",
        type=int,
        required=False,
        default=0,
        help="remaining life time, unit: hours",
    )

    parser.add_argument(
        "-n",
        "--num",
        type=int,
        required=False,
        default=64,
        help="threads num for check proxy",
    )

    parser.add_argument(
        "-o",
        "--overwrite",
        dest="overwrite",
        action="store_true",
        default=False,
        help="overwrite domains",
    )

    parser.add_argument(
        "-p",
        "--pages",
        type=int,
        required=False,
        default=sys.maxsize,
        help="max page number when crawling telegram",
    )

    parser.add_argument(
        "-r",
        "--refresh",
        dest="refresh",
        action="store_true",
        default=False,
        help="refresh and remove expired proxies with existing subscriptions",
    )

    parser.add_argument(
        "-s",
        "--skip",
        dest="skip",
        action=argparse.BooleanOptionalAction,
        default=utils.env_bool("SKIP_ALIVE_CHECK", False),
        help="skip usability checks",
    )

    parser.add_argument(
        "-t",
        "--targets",
        nargs="+",
        choices=subconverter.CONVERT_TARGETS,
        default=["clash", "v2ray", "singbox"],
        help=f"choose one or more generated profile type. default to clash, v2ray and singbox. supported: {subconverter.CONVERT_TARGETS}",
    )

    parser.add_argument(
        "-u",
        "--url",
        type=str,
        required=False,
        default="https://www.google.com/generate_204",
        help="test url",
    )

    parser.add_argument(
        "-v",
        "--ignore-default-filters",
        dest="ignore_default_filters",
        action="store_true",
        default=False,
        help="ignoring default proxies filter rules",
    )

    parser.add_argument(
        "-y",
        "--custom-sites",
        type=str,
        required=False,
        default=os.environ.get("CUSTOMIZE_LINK", ""),
        help="the url to the list of airports that you maintain yourself",
    )

    aggregate(args=parser.parse_args())
