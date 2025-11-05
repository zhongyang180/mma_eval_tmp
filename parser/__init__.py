import logging
from pathlib import Path

from .doctree import DoctreeParser
from .parse_dp import parse_dp
from .parse_excel import analyze_excel


logger = logging.getLogger("parser")
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

def process_file(
    file_path: str,
    api_key: str,
    api_base: str,
    deployment_name: str,
    api_version: str = "2024-02-15-preview",
    is_semantic_chunk: bool = False,
    embedding_base: str = None,
    embedding_api_key: str = None,
    embedding_model: str = "",
):
    """
    根据文件类型自动选择合适的解析方式：
      - Markdown / TXT → 层次化结构提取 + 可选语义分块
      - DOCX / PDF → MinerU 文档结构化 + 语义树化
      - Excel → 表格 JSON 解析 + 单元格三元组提取
    """

    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"文件不存在: {file_path}")

    ext = file_path.suffix.lower()
    logger.info(f"正在处理文件: {file_path.name} ({ext})")

    parser = DoctreeParser(
        api_key=api_key,
        api_base=api_base,
        api_version=api_version,
        deployment_name=deployment_name,
    )

    try:
        if ext in [".md", ".txt"]:
            logger.info("解析 Markdown/Text 文档")
            text = file_path.read_text(encoding="utf-8")
            result = parser.parse(text)
            logger.info("文档解析完成")
            return {"type": "text", "file": str(file_path), "data": result}

        elif ext in [".docx", ".pdf"]:
            logger.info("调用 MinerU 接口解析 DOCX/PDF")
            md_content = parse_dp(str(file_path))
            logger.info("MinerU 解析完成，执行 Doctree 树化分析")
            result = parser.parse(md_content)
            logger.info("文档结构化完成")
            return {"type": "document", "file": str(file_path), "data": result}

        elif ext in [".xlsx", ".xls"]:
            logger.info("调用 Excel 三元组提取模块")
            excel_json = analyze_excel(str(file_path))
            triplets = parser.parse(excel_json)
            logger.info(f"Excel 解析完成，提取 {len(triplets)} 个单元格三元组")
            return {"type": "excel", "file": str(file_path), "data": triplets}

        else:
            raise ValueError(f"暂不支持的文件类型: {ext}")

    except Exception as e:
        logger.exception(f"处理文件 {file_path.name} 失败: {e}")
        raise


__all__ = ["process_file", "DoctreeParser"]
