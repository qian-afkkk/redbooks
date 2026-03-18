#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
小红书爬虫 CLI 接口
供 API 和外部脚本调用
"""

import sys
import argparse
from crawler_ultimate import CrawlerApp, CrawlerConfig
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="小红书爬虫 CLI")
    parser.add_argument('--keyword', default='', help='搜索关键词')
    parser.add_argument('--count', type=int, default=10, help='爬取数量')
    parser.add_argument('--mode', default='standard', help='爬取模式')
    parser.add_argument('--output-dir', default='./output', help='输出目录')
    parser.add_argument('--no-gui', action='store_true', help='无 GUI 模式')

    args = parser.parse_args()

    # 创建输出目录
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # 创建爬虫应用（headless 模式）
    app = CrawlerApp(headless=True)

    # 配置爬虫
    app.config.keyword = args.keyword
    app.config.max_notes = args.count
    app.config.save_cookies = True

    print(f"开始爬取: {args.keyword or '主页推荐'}, 目标 {args.count} 条")

    try:
        # 执行爬取
        app.run_cli_crawl(args.keyword, args.count)

        # 等待完成
        while app.is_running:
            import time
            time.sleep(0.5)

        print(f"爬取完成！共获取 {len(app.results)} 条笔记")

        # 导出结果
        if app.results:
            from datetime import datetime
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            keyword = args.keyword or 'feed'
            excel_file = output_dir / f"result_{keyword}_{timestamp}.xlsx"
            app.export_to_excel(str(excel_file))
            print(f"结果已导出: {excel_file}")
            return 0
        else:
            print("未获取到数据")
            return 1

    except Exception as e:
        print(f"爬取失败: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
