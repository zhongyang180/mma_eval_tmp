# -*- coding: utf-8 -*-
"""
批量或单文件调用宁德内部 MinerU API，将 DOCX 转为 PDF 并解析为 Markdown。
Author: HuangZJ @ CATL
"""

import io
import json
import logging
import subprocess
import tempfile
from pathlib import Path
from typing import List, Dict
import requests
import argparse

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("MinerUParser")

MINERU_API_URL = "http://mineru:8000/file_parse"
DEFAULT_BATCH_SIZE = 5


def docx_to_pdf(file_path: str) -> str:
    """
    将 DOCX 转为 PDF，返回持久化的 PDF 文件路径
    """
    tmpdir = tempfile.mkdtemp()
    input_docx = Path(file_path)
    output_pdf = Path(tmpdir) / f"{input_docx.stem}.pdf"

    cmd = [
        "libreoffice",
        "--headless",
        "--convert-to", "pdf",
        "--outdir", tmpdir,
        str(input_docx),
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            raise RuntimeError(result.stderr)

        if not output_pdf.exists():
            raise FileNotFoundError(f"PDF 文件未生成: {output_pdf}")

        return str(output_pdf)

    except subprocess.TimeoutExpired:
        raise RuntimeError("LibreOffice 转换超时 (>60s)")
    except Exception as e:
        raise RuntimeError(f"DOCX 转换 PDF 失败: {e}")


def call_file_parse(pdf_paths: List[str]) -> Dict[str, str]:
    """
    调用 MinerU 接口进行解析，返回 {文件名: Markdown文本}
    """
    files = [("files", open(path, "rb")) for path in pdf_paths]
    data = {
        "return_md": True,
        "return_middle_json": False,
        "response_format_zip": False,
        "lang_list": ["ch"],
        "table_enable": True,
        "formula_enable": True,
    }

    logger.info(f"调用 MinerU API: {MINERU_API_URL}")
    try:
        resp = requests.post(MINERU_API_URL, files=files, data=data)
        resp.raise_for_status()
        result = resp.json()
    except Exception as e:
        raise RuntimeError(f"调用 MinerU 解析接口失败: {e}")
    finally:
        for _, f in files:
            f.close()

    markdown_data = {}
    if isinstance(result, dict) and "results" in result:
        for file_name, content in result["results"].items():
            markdown_data[file_name] = content.get("md_content", "")
    else:
        logger.warning(f"返回结果结构异常: {json.dumps(result, ensure_ascii=False)}")

    logger.info(f"文件解析完成，共 {len(markdown_data)} 个结果")

    return markdown_data


def parse_dp(file_path: str) -> Dict[str, str]:
    """
    单文件处理：DOCX 或 PDF
    返回 {filename: markdown}
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"文件不存在: {file_path}")

    logger.info(f"📄 开始处理文件: {file_path.name}")

    if file_path.suffix.lower() == ".docx":
        logger.info("检测到 DOCX 文件，执行转换...")
        pdf_path = docx_to_pdf(str(file_path))
    elif file_path.suffix.lower() == ".pdf":
        pdf_path = str(file_path)
    else:
        raise ValueError("仅支持 .docx 或 .pdf 文件")

    logger.info(f"调用 MinerU 接口解析: {pdf_path}")
    md_data = call_file_parse([pdf_path])

    return md_data[file_path.stem]


def process_file(input_dir: str, output_dir: str = "tmp", batch_size: int = DEFAULT_BATCH_SIZE):
    """
    批量处理文件：将 input_dir 下的 .docx 文件批量解析为 Markdown
    """
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    pdf_dir = output_dir / "pdf"
    md_dir = output_dir / "md"
    pdf_dir.mkdir(parents=True, exist_ok=True)
    md_dir.mkdir(parents=True, exist_ok=True)

    docx_files = list(input_dir.glob("*.docx"))
    total_files = len(docx_files)
    logger.info(f"共检测到 {total_files} 个 DOCX 文件")

    for i in range(0, total_files, batch_size):
        batch_files = docx_files[i:i + batch_size]
        logger.info(f"处理第 {i // batch_size + 1} 批，共 {len(batch_files)} 个文件")

        pdf_paths = []
        for docx_file in batch_files:
            try:
                pdf_path = docx_to_pdf(str(docx_file))
                saved_pdf = pdf_dir / f"{docx_file.stem}.pdf"
                Path(pdf_path).replace(saved_pdf)
                pdf_paths.append(str(saved_pdf))
                logger.info(f"已保存 PDF: {saved_pdf}")
            except Exception as e:
                logger.error(f"转换失败 [{docx_file.name}]: {e}")

        if not pdf_paths:
            continue

        try:
            markdown_data = call_file_parse(pdf_paths)
        except Exception as e:
            logger.error(f"MinerU 调用失败: {e}")
            continue

        for pdf_path in pdf_paths:
            file_name = Path(pdf_path).stem
            md_text = markdown_data.get(file_name, "")
            if md_text:
                md_file = md_dir / f"{file_name}.md"
                with open(md_file, "w", encoding="utf-8") as f:
                    f.write(md_text)
                logger.info(f"已保存 Markdown: {md_file}")
            else:
                logger.warning(f"未能解析文件 {file_name}")

    logger.info("所有文件处理完成")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="批量调用宁德 MinerU API 进行文档解析")
    parser.add_argument("input_dir", help="输入文件夹路径，包含 DOCX 文件")
    parser.add_argument("output_dir", help="输出文件夹路径，保存 PDF 和 Markdown 文件")
    parser.add_argument("--batch_size", type=int, default=DEFAULT_BATCH_SIZE, help="每批处理文件数 (默认5)")
    args = parser.parse_args()

    process_file(args.input_dir, args.output_dir, args.batch_size)


    parse_dp("/app/test.docx") 