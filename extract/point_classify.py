import os
import json
import logging
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

from extract.chat import chat_infer
from extract.prompts import classify_point_prompt
from parser.doctree import DoctreeParser


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def run_point_extraction(
    json_path: str,
    api_key: str,
    api_base: str,
    deployment_name: str = "gpt-4o",
    save_dir: str = "./tmp/results",
    max_workers: int = 6,
):
    """
    发明点/知识点分类主函数（支持多线程并发）：
    1. 读取带语义分块的 JSON 文件
    2. 调用 DoctreeParser 序列化结构
    3. 多线程并发调用大模型分类
    4. 保存结果为 JSON / JSONL
    """

    parser = DoctreeParser(
        api_key=api_key,
        api_base=api_base,
        deployment_name=deployment_name,
    )

    logger.info(f"Loading JSON file: {json_path}")
    with open(json_path, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    logger.info("Serializing content with titles...")
    parsed_data = raw_data.get("data", raw_data)
    data = parser.serialize_with_titles(parsed_data)

    total = len(data)
    logger.info(f"Total chunks: {total}")

    def process_item(i, item):
        chunk = f"""
        【本段文本所在位置（本段文本的层级标题）】
        {item['title_path']}

        【文本】
        {item['text']}
        """
        prompt = classify_point_prompt.replace("__SEGMENT__", chunk)
        try:
            response = chat_infer(prompt, LLM_Type="gpt-4o")
            response = response.replace("```", "").replace("json", "").strip()
            return {
                "index": i,
                "title_path": item["title_path"],
                "text": item["text"],
                "result": response
            }
        except Exception as e:
            logger.error(f"Chunk {i} failed: {e}")
            return {
                "index": i,
                "title_path": item["title_path"],
                "text": item["text"],
                "result": f"ERROR: {e}"
            }

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_item, i, item): i
            for i, item in enumerate(data, 1)
        }

        for future in tqdm(as_completed(futures), total=len(futures), desc="Classifying segments", unit="chunk"):
            result = future.result()
            results.append(result)

    os.makedirs(save_dir, exist_ok=True)

    full_json_path = os.path.join(save_dir, f"{os.path.basename(json_path)}_result_full.json")

    logger.info(f"Saving full results to {full_json_path}")
    with open(full_json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    logger.info("Classification and saving completed.")
    return results

def run_excel_extraction(
    json_path: str,
    save_dir: str = "./tmp/results",
    max_workers: int = 6,
):

    logger.info(f"Loading Excel JSON file: {json_path}")
    with open(json_path, "r", encoding="utf-8") as f:
        excel_json = json.load(f)

    data = excel_json.get("data", [])
    logger.info(f"Total triplets: {len(data)}")

    def process_triplet(i, item):
        """单条三元组处理"""
        chunk = f"""
            【文本的行标签】
            {item.get('row_label', '')}

            【文本的列标签】
            {item.get('col_label', '')}

            【文本】
            {item.get('value', '')}
        """.strip()

        prompt = classify_point_prompt.replace("__SEGMENT__", chunk)

        try:
            response = chat_infer(prompt, LLM_Type="gpt-4o")
            response = response.replace("```", "").replace("json", "").strip()
            return {
                "index": i,
                "row_label": item.get("row_label"),
                "col_label": item.get("col_label"),
                "value": item.get("value"),
                "result": response,
            }
        except Exception as e:
            logger.error(f"Triplet {i} failed: {e}")
            return {
                "index": i,
                "row_label": item.get("row_label"),
                "col_label": item.get("col_label"),
                "value": item.get("value"),
                "result": f"ERROR: {e}",
            }

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_triplet, i, item): i
            for i, item in enumerate(data, 1)
        }

        for future in tqdm(as_completed(futures), total=len(futures), desc="Classifying triplets", unit="row"):
            results.append(future.result())

    os.makedirs(save_dir, exist_ok=True)

    full_json_path = os.path.join(save_dir, f"excel_result_full.json")
    logger.info(f"Saving full results to {full_json_path}")
    with open(full_json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    logger.info("Excel extraction completed.")
    return results

if __name__ == "__main__":
    results = run_excel_extraction(
        json_path="/data5/sby/mma_eval/tmp/test_2.json",
        # api_key="ae0550db705443238dd2595a58cd964c",
        # api_base="https://ustc-law-gpt4-1.openai.azure.com",
        # deployment_name="gpt-4o",
        save_dir="/data5/sby/mma_eval/tmp/results",
        max_workers=64,
    )

    print(json.dumps(results[:3], ensure_ascii=False, indent=2))
