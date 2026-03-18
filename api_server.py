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
    """爬取请求模型"""
    keyword: str = Field(..., description="搜索关键词")
    count: int = Field(10, ge=1, le=100, description="爬取数量")
    mode: str = Field("standard", description="爬取模式: standard/fast")
    crawl_type: str = Field("keyword", description="爬取类型: keyword/blogger/hot")
    blogger_url: Optional[str] = Field(None, description="博主主页URL")
    download_images: bool = Field(True, description="是否下载图片")
    download_videos: bool = Field(True, description="是否下载视频")
    task_id: Optional[str] = Field(None, description="任务ID（可选）")

    class Config:
        json_schema_extra = {
            "example": {
                "keyword": "护肤品",
                "count": 10,
                "mode": "standard",
                "crawl_type": "keyword"
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

    def run_crawl(self, task_id: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """执行爬取任务"""
        logger.info(f"[{task_id}] 开始爬取: {params}")

        try:
            task_manager.update_task(task_id, status="running", progress=0, message="初始化爬虫...")

            # 导入爬虫模块
            from crawler_ultimate import XiaohongshuCrawler, CrawlerConfig

            # 创建配置
            config = CrawlerConfig(
                keyword=params.get('keyword', ''),
                max_notes=params.get('count', 10),
                crawl_mode=params.get('mode', 'standard'),
                crawl_type=params.get('crawl_type', 'keyword'),
                blogger_url=params.get('blogger_url', ''),
                download_images=params.get('download_images', True),
                download_videos=params.get('download_videos', True),
            )

            # 创建爬虫实例
            crawler = XiaohongshuCrawler(config)

            # 设置进度回调
            def progress_callback(current, total, message):
                progress = int((current / total) * 100) if total > 0 else 0
                task_manager.update_task(task_id, progress=progress, message=message)
                logger.info(f"[{task_id}] 进度: {progress}% - {message}")

            crawler.progress_callback = progress_callback

            # 执行爬取
            task_manager.update_task(task_id, message="正在爬取...")
            results = crawler.run()

            if not results:
                raise Exception("爬取失败，未获取到数据")

            # 导出结果
            task_manager.update_task(task_id, progress=80, message="正在导出数据...")

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            keyword = params.get('keyword', 'unknown')

            # 导出 Excel
            excel_filename = f"result_{keyword}_{timestamp}.xlsx"
            excel_path = OUTPUT_DIR / excel_filename
            crawler.export_to_excel(str(excel_path))

            # 收集媒体文件
            media_files = self._collect_media_files(keyword, timestamp)

            result = {
                "success": True,
                "task_id": task_id,
                "keyword": keyword,
                "count": len(results),
                "excel_file": str(excel_path),
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

            logger.info(f"[{task_id}] 爬取完成: {len(results)} 条记录")
            return result

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
async def upload_to_feishu(task_id: str):
    """
    上传结果到飞书多维表格

    - **task_id**: 任务ID
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
        upload_result = uploader.upload_crawl_result(result)

        # 更新任务结果
        task["result"]["feishu_uploaded"] = True
        task["result"]["feishu_url"] = upload_result.get("url")

        return {
            "success": True,
            "task_id": task_id,
            "upload_result": upload_result
        }

    except Exception as e:
        logger.error(f"飞书上传失败: {e}")
        raise HTTPException(status_code=500, detail=f"上传失败: {str(e)}")


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
