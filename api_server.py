#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Redbooks API Server
为 OpenClaw 技能提供 REST API 接口
"""

import os
import sys
import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import uvicorn

# 添加项目路径
PROJECT_DIR = Path(__file__).parent
sys.path.insert(0, str(PROJECT_DIR))

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(PROJECT_DIR / 'data' / 'api.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# API 配置
API_HOST = "127.0.0.1"
API_PORT = 8848
API_WORKERS = 1

# 数据目录
DATA_DIR = PROJECT_DIR / 'data'
IMAGES_DIR = PROJECT_DIR / 'images'
OUTPUT_DIR = PROJECT_DIR / 'output'

# 确保目录存在
for dir_path in [DATA_DIR, IMAGES_DIR, OUTPUT_DIR]:
    dir_path.mkdir(exist_ok=True)


# ============== 请求模型 ==============

class CrawlRequest(BaseModel):
    """爬取请求模型 - 支持所有 GUI 功能"""
    # 基础参数
    keyword: str = Field("", description="搜索关键词（为空表示主页推荐）")
    count: int = Field(10, ge=1, le=500, description="爬取数量")
    mode: str = Field("standard", description="爬取模式: standard/fast/turbo")
    crawl_type: str = Field("keyword", description="爬取类型: keyword/blogger/hot")
    blogger_url: Optional[str] = Field(None, description="博主主页URL（blogger模式必填）")

    # 内容选项
    download_images: bool = Field(True, description="是否下载图片")
    download_videos: bool = Field(True, description="是否下载视频")
    get_comments: bool = Field(True, description="是否获取评论")
    comments_count: int = Field(20, ge=0, le=100, description="评论数量")

    # 筛选条件
    min_likes: int = Field(0, ge=0, description="最小点赞数筛选")
    max_likes: int = Field(999999, ge=0, description="最大点赞数筛选")
    note_type: str = Field("全部", description="笔记类型: 全部/图文/视频")
    date_filter: str = Field("全部", description="日期筛选")

    # 速度控制
    scroll_times: int = Field(10, ge=1, le=50, description="滚动次数")

    task_id: Optional[str] = Field(None, description="任务ID（可选）")

    class Config:
        json_schema_extra = {
            "example": {
                "keyword": "护肤品",
                "count": 10,
                "mode": "standard",
                "min_likes": 100,
                "note_type": "视频"
            }
        }


class TaskStatusResponse(BaseModel):
    """任务状态响应"""
    task_id: str
    status: str  # pending/running/completed/failed
    progress: int
    message: str
    result: Optional[Dict[str, Any]] = None


class CrawlResult(BaseModel):
    """爬取结果"""
    success: bool
    task_id: str
    keyword: str
    count: int
    excel_file: str
    images_dir: str
    video_files: List[str]
    feishu_uploaded: bool = False
    feishu_url: Optional[str] = None


# ============== 任务管理 ==============

class TaskManager:
    """任务管理器"""

    def __init__(self):
        self.tasks: Dict[str, Dict[str, Any]] = {}
        self.executor = ThreadPoolExecutor(max_workers=2)

    def create_task(self, task_id: str, params: Dict[str, Any]) -> str:
        """创建新任务"""
        self.tasks[task_id] = {
            "status": "pending",
            "progress": 0,
            "message": "任务已创建",
            "result": None,
            "error": None,
            "created_at": datetime.now().isoformat(),
            "params": params
        }
        return task_id

    def update_task(self, task_id: str, **kwargs):
        """更新任务状态"""
        if task_id in self.tasks:
            self.tasks[task_id].update(kwargs)
            self.tasks[task_id]["updated_at"] = datetime.now().isoformat()

    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取任务状态"""
        return self.tasks.get(task_id)


task_manager = TaskManager()


# ============== 爬虫执行器 ==============

class CrawlerExecutor:
    """爬虫执行器"""

    def __init__(self):
        self.crawler_dir = PROJECT_DIR
        self.executor = task_manager.executor

    def run_crawl(self, task_id: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """执行爬取任务（使用 subprocess 调用现有爬虫）"""
        logger.info(f"[{task_id}] 开始爬取: {params}")

        import subprocess

        try:
            task_manager.update_task(task_id, status="running", progress=0, message="初始化爬虫...")

            keyword = params.get('keyword', '')
            count = params.get('count', 10)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

            # 使用 subprocess 调用爬虫（CLI 模式）
            task_manager.update_task(task_id, progress=10, message="启动爬虫进程...")

            # 构建命令（支持所有参数）
            cmd = [
                'python3', str(PROJECT_DIR / 'cli_crawler.py'),
                '--keyword', keyword,
                '--count', str(count),
                '--output-dir', str(OUTPUT_DIR),
                '--mode', params.get('mode', 'standard'),
                '--crawl-type', params.get('crawl_type', 'keyword'),
                '--min-likes', str(params.get('min_likes', 0)),
                '--max-likes', str(params.get('max_likes', 999999)),
                '--note-type', params.get('note_type', '全部'),
                '--scroll-times', str(params.get('scroll_times', 10)),
            ]

            # 添加可选参数
            if params.get('blogger_url'):
                cmd.extend(['--blogger-url', params['blogger_url']])
            if not params.get('download_images', True):
                cmd.append('--no-images')
            if not params.get('download_videos', True):
                cmd.append('--no-videos')
            if not params.get('get_comments', True):
                cmd.append('--no-comments')
            if params.get('comments_count', 20) != 20:
                cmd.extend(['--comments-count', str(params['comments_count'])])

            logger.info(f"执行命令: {' '.join(cmd)}")

            # 执行爬虫（超时 10 分钟）
            result = subprocess.run(
                cmd,
                cwd=str(PROJECT_DIR),
                capture_output=True,
                text=True,
                timeout=600
            )

            if result.returncode != 0:
                error_msg = result.stderr or result.stdout or "未知错误"
                raise Exception(f"爬虫执行失败: {error_msg[:200]}")

            task_manager.update_task(task_id, progress=90, message="整理结果...")

            # 查找结果文件
            excel_file = self._find_latest_result(keyword)
            if not excel_file:
                raise Exception("未找到结果文件")

            # 收集媒体文件
            media_files = self._collect_media_files(keyword, timestamp)

            # 获取记录数
            try:
                import pandas as pd
                df = pd.read_excel(excel_file)
                count = len(df)
            except:
                count = 0

            result = {
                "success": True,
                "task_id": task_id,
                "keyword": keyword,
                "count": count,
                "excel_file": str(excel_file),
                "images_dir": media_files["images_dir"],
                "video_files": media_files["videos"],
                "timestamp": timestamp
            }

            task_manager.update_task(
                task_id,
                status="completed",
                progress=100,
                message="爬取完成",
                result=result
            )

            logger.info(f"[{task_id}] 爬取完成: {count} 条记录")
            return result

        except subprocess.TimeoutExpired:
            logger.error(f"[{task_id}] 爬取超时")
            task_manager.update_task(
                task_id,
                status="failed",
                message="爬取超时（10分钟）",
                error="timeout"
            )
            return {"success": False, "task_id": task_id, "error": "timeout"}

        except Exception as e:
            logger.error(f"[{task_id}] 爬取失败: {e}")
            task_manager.update_task(
                task_id,
                status="failed",
                message=f"爬取失败: {str(e)}",
                error=str(e)
            )
            return {
                "success": False,
                "task_id": task_id,
                "error": str(e)
            }

    def _find_latest_result(self, keyword: str) -> Optional[str]:
        """查找最新的结果文件"""
        files = list(OUTPUT_DIR.glob("result_*.xlsx"))
        if files:
            # 按修改时间排序，返回最新的
            latest = max(files, key=lambda f: f.stat().st_mtime)
            logger.info(f"找到结果文件: {latest}")
            return str(latest)
        return None

    def _collect_media_files(self, keyword: str, timestamp: str) -> Dict[str, Any]:
        """收集媒体文件"""
        images_dir = IMAGES_DIR / f"{keyword}_{timestamp}"
        videos = []

        if images_dir.exists():
            # 查找所有视频文件
            for video_file in images_dir.rglob("*.mp4"):
                videos.append(str(video_file))

        return {
            "images_dir": str(images_dir) if images_dir.exists() else "",
            "videos": videos
        }


crawler_executor = CrawlerExecutor()


# ============== FastAPI 应用 ==============

app = FastAPI(
    title="Redbooks API",
    description="小红书爬虫 API 服务",
    version="1.0.0"
)


@app.get("/")
async def root():
    """根路径"""
    return {
        "service": "Redbooks API",
        "version": "1.0.0",
        "status": "running"
    }


@app.get("/health")
async def health():
    """健康检查"""
    return {"status": "healthy"}


@app.post("/crawl", response_model=CrawlResult)
async def crawl(request: CrawlRequest, background_tasks: BackgroundTasks):
    """
    启动爬取任务

    - **keyword**: 搜索关键词
    - **count**: 爬取数量 (1-100)
    - **mode**: 爬取模式 (standard/fast)
    - **crawl_type**: 爬取类型 (keyword/blogger/hot)
    """
    # 生成任务ID
    task_id = request.task_id or f"task_{datetime.now().strftime('%Y%m%d%H%M%S')}"

    # 创建任务
    params = request.model_dump()
    task_manager.create_task(task_id, params)

    # 在后台执行爬取
    loop = asyncio.get_event_loop()
    background_tasks.add_task(
        lambda: loop.run_in_executor(
            crawler_executor.executor,
            crawler_executor.run_crawl,
            task_id,
            params
        )
    )

    return CrawlResult(
        success=True,
        task_id=task_id,
        keyword=request.keyword,
        count=request.count,
        excel_file="",
        images_dir="",
        video_files=[]
    )


@app.get("/task/{task_id}", response_model=TaskStatusResponse)
async def get_task_status(task_id: str):
    """获取任务状态"""
    task = task_manager.get_task(task_id)

    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")

    return TaskStatusResponse(
        task_id=task_id,
        status=task.get("status", "unknown"),
        progress=task.get("progress", 0),
        message=task.get("message", ""),
        result=task.get("result")
    )


@app.get("/tasks")
async def list_tasks():
    """列出所有任务"""
    return {
        "tasks": [
            {
                "task_id": tid,
                **task
            }
            for tid, task in task_manager.tasks.items()
        ]
    }


@app.post("/upload/feishu")
async def upload_to_feishu(task_id: str, upload_images: bool = True):
    """
    上传结果到飞书多维表格

    - **task_id**: 任务ID
    - **upload_images**: 是否上传图片（默认 True）
    """
    task = task_manager.get_task(task_id)

    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")

    if task.get("status") != "completed":
        raise HTTPException(status_code=400, detail="任务未完成")

    result = task.get("result")
    if not result:
        raise HTTPException(status_code=400, detail="任务没有结果")

    try:
        # 导入飞书上传模块
        from feishu_uploader import FeishuUploader

        uploader = FeishuUploader()

        # 1. 上传 Excel 数据到多维表格
        upload_result = uploader.upload_crawl_result(result)

        # 2. 如果有图片，使用 feishu-upload 技能上传
        image_urls = []
        images_dir = result.get("images_dir", "")

        if upload_images and images_dir and os.path.exists(images_dir):
            logger.info(f"开始上传图片: {images_dir}")
            image_urls = await _upload_images_to_feishu(images_dir)

            # 将图片链接添加到上传结果
            if image_urls:
                upload_result["image_urls"] = image_urls
                upload_result["images_uploaded"] = len(image_urls)

        # 更新任务结果
        task["result"]["feishu_uploaded"] = True
        task["result"]["feishu_url"] = upload_result.get("url")
        task["result"]["image_urls"] = image_urls

        return {
            "success": True,
            "task_id": task_id,
            "upload_result": upload_result,
            "images_uploaded": len(image_urls)
        }

    except Exception as e:
        logger.error(f"飞书上传失败: {e}")
        raise HTTPException(status_code=500, detail=f"上传失败: {str(e)}")


async def _upload_images_to_feishu(images_dir: str) -> List[str]:
    """
    使用 feishu-upload 技能上传图片

    Args:
        images_dir: 图片目录路径

    Returns:
        图片 URL 列表
    """
    import subprocess
    from pathlib import Path

    upload_script = Path.home() / ".openclaw/workspace/skills/feishu-upload/scripts/upload.py"

    if not upload_script.exists():
        logger.warning(f"上传脚本不存在: {upload_script}")
        return []

    # 查找所有图片
    image_files = list(Path(images_dir).rglob("*.jpg"))
    image_files.extend(Path(images_dir).rglob("*.png"))
    image_files.extend(Path(images_dir).rglob("*.jpeg"))

    if not image_files:
        logger.info("没有找到图片文件")
        return []

    # 限制上传数量（最多 20 张）
    image_files = image_files[:20]

    logger.info(f"准备上传 {len(image_files)} 张图片")

    try:
        # 调用上传脚本
        result = subprocess.run(
            ["python3", str(upload_script)] + [str(f) for f in image_files] + ["--output", "json"],
            capture_output=True,
            text=True,
            timeout=300
        )

        if result.returncode == 0:
            import json
            output = json.loads(result.stdout)
            urls = output.get("urls", [])
            logger.info(f"图片上传成功: {len(urls)} 张")
            return urls
        else:
            logger.warning(f"图片上传失败: {result.stderr}")
            return []

    except Exception as e:
        logger.error(f"图片上传异常: {e}")
        return []


# ============== 主程序 ==============

def main():
    """启动 API 服务器"""
    logger.info(f"启动 Redbooks API 服务器 @ http://{API_HOST}:{API_PORT}")

    uvicorn.run(
        app,
        host=API_HOST,
        port=API_PORT,
        log_level="info",
        access_log=True
    )


if __name__ == "__main__":
    main()
