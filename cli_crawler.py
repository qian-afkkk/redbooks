#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
小红书爬虫 CLI 接口
供 API 和外部脚本调用
支持所有 GUI 功能
"""

import sys
import argparse
import os
from datetime import datetime
from pathlib import Path
from crawler_ultimate import CrawlerApp, CrawlerConfig


def main():
    parser = argparse.ArgumentParser(description="小红书爬虫 CLI")
    parser.add_argument('--keyword', default='', help='搜索关键词')
    parser.add_argument('--count', type=int, default=10, help='爬取数量')
    parser.add_argument('--mode', default='standard', choices=['standard', 'fast', 'turbo'], help='爬取模式')
    parser.add_argument('--crawl-type', default='keyword', choices=['keyword', 'blogger', 'hot'], help='爬取类型')
    parser.add_argument('--blogger-url', default='', help='博主主页 URL')
    parser.add_argument('--output-dir', default='./output', help='输出目录')

    # 筛选条件
    parser.add_argument('--min-likes', type=int, default=0, help='最小点赞数')
    parser.add_argument('--max-likes', type=int, default=999999, help='最大点赞数')
    parser.add_argument('--note-type', default='全部', choices=['全部', '图文', '视频'], help='笔记类型筛选')
    parser.add_argument('--date-filter', default='全部', help='日期筛选')

    # 内容选项
    parser.add_argument('--no-images', action='store_true', help='不下载图片')
    parser.add_argument('--no-videos', action='store_true', help='不下载视频')
    parser.add_argument('--no-comments', action='store_true', help='不获取评论')
    parser.add_argument('--comments-count', type=int, default=20, help='评论数量')

    # 速度控制
    parser.add_argument('--scroll-times', type=int, default=10, help='滚动次数')
    parser.add_argument('--click-delay', type=float, default=0.3, help='点击延迟')

    args = parser.parse_args()

    # 创建输出目录
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # 创建爬虫应用（headless 模式）
    app = CrawlerApp(headless=True)

    # 配置爬虫（完整配置）
    app.config.keyword = args.keyword
    app.config.max_notes = args.count
    app.config.crawl_mode = args.mode
    app.config.crawl_type = args.crawl_type
    app.config.blogger_url = args.blogger_url

    # 筛选条件
    app.config.min_likes = args.min_likes
    app.config.max_likes = args.max_likes
    app.config.note_type_filter = args.note_type
    app.config.date_filter = args.date_filter

    # 内容选项
    app.config.download_images = not args.no_images
    app.config.download_videos = not args.no_videos
    app.config.get_comments = not args.no_comments
    app.config.comments_count = args.comments_count

    # 速度控制
    app.config.scroll_times = args.scroll_times
    app.config.save_cookies = True

    display_keyword = args.keyword or ('博主' if args.crawl_type == 'blogger' else '主页推荐')

    print(f"🚀 开始爬取: {display_keyword}, 目标 {args.count} 条")
    print(f"   模式: {args.mode}, 类型: {args.note_type}, 点赞: {args.min_likes}-{args.max_likes}")

    try:
        # 执行爬取
        app.run_cli_crawl(args.keyword, args.count)

        # 等待完成
        while app.is_running:
            import time
            time.sleep(0.5)

        # 使用 run_cli_crawl 的结果
        notes_count = len(app.all_notes_data)

        # 查找生成的 Excel 文件（run_cli_crawl 已经生成了）
        excel_path = None
        try:
            import glob
            data_dir = "data"
            if os.path.exists(data_dir):
                xlsx_files = glob.glob(os.path.join(data_dir, f"搜索结果_{display_keyword}_*.xlsx"))
                if xlsx_files:
                    excel_path = max(xlsx_files, key=os.path.getmtime)

            # 如果 data 目录没有，检查 output 目录
            if not excel_path:
                xlsx_files = list(output_dir.glob("result_*.xlsx"))
                if xlsx_files:
                    excel_path = max(xlsx_files, key=lambda f: f.stat().st_mtime)
        except Exception as e:
            print(f"查找 Excel 文件时出错: {e}")

        if excel_path:
            print(f"\n✅ 爬取完成！共获取 {notes_count} 条笔记")
            print(f"📄 Excel 文件: {os.path.abspath(excel_path)}")

            # 返回 Excel 路径（供 API 调用）
            if os.path.abspath(excel_path).startswith(str(output_dir)):
                # 如果已经在 output 目录，直接返回
                print(f"OUTPUT:{excel_path}")
            else:
                # 复制到 output 目录
                import shutil
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                keyword = args.keyword or 'feed'
                dest_file = output_dir / f"result_{keyword}_{timestamp}.xlsx"
                shutil.copy2(excel_path, dest_file)
                print(f"📋 已复制到输出目录: {dest_file}")
                print(f"OUTPUT:{dest_file}")

            return 0
        else:
            print(f"\n⚠️  爬取完成但未找到 Excel 文件")
            print(f"📊 共获取 {notes_count} 条笔记")
            return 1 if notes_count == 0 else 0

    except Exception as e:
        print(f"❌ 爬取失败: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
