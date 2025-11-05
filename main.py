import os
import glob
import json

from pathlib import Path

from parser import process_file
from extract.point_classify import run_point_extraction, run_excel_extraction

def process_folder(input_folder, output_folder, tmp_folder, LLM_Type):
    """处理文件夹中的所有docx文件"""
    os.makedirs(output_folder, exist_ok=True)
    
    # 获取所有文件
    files = glob.glob(os.path.join(input_folder, "*"))
    
    if not files:
        print(f"在 {input_folder} 中未找到任何文件")
        return
    
    for file_path in files:
        path = Path(file_path)
        print(f"\n 处理文档: {path.name}")

        ext = path.suffix.lower()
        data = process_file(
            file_path=str(path),
            api_key="ae0550db705443238dd2595a58cd964c",
            api_base="https://ustc-law-gpt4-1.openai.azure.com",
            deployment_name="gpt-4o",
        )
        json_path = os.path.join(tmp_folder, f"{path.stem}.json")
        # data = parser.serialize_with_titles(data)
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        if ext in [".pdf", ".docx", ".md", ".txt"]:

            result = run_point_extraction(
                json_path=json_path,
                api_key="ae0550db705443238dd2595a58cd964c",
                api_base="https://ustc-law-gpt4-1.openai.azure.com",
                deployment_name="gpt-4o",
                save_dir=output_folder,
                max_workers=64,
            )
        elif ext in [".xlsx", ".xls"]:
            result = run_excel_extraction(
                json_path=json_path,
                # api_key="ae0550db705443238dd2595a58cd964c",
                # api_base="https://ustc-law-gpt4-1.openai.azure.com",
                # deployment_name="gpt-4o",
                save_dir=output_folder,
                max_workers=64,
            )
        else:
            print(f"跳过不支持的文件类型: {path.name}")
            continue

        # 保存输出
        out_dir = path.parent / "results"
        out_dir.mkdir(exist_ok=True)
        out_file = out_dir / f"{path.stem}_result.json"

        with out_file.open("w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    # result = process_file(
    #     "/app/mma_eval_tmp/test_data/source_file/P2023110253CN1.docx",
    #     api_key="ae0550db705443238dd2595a58cd964c",
    #     api_base="https://ustc-law-gpt4-1.openai.azure.com",
    #     deployment_name="gpt-4o",
    #     # is_semantic_chunk=True,
    #     # embedding_base="https://api.chatanywhere.tech",
    #     # embedding_api_key="sk-xxxxxx",
    #     # embedding_model="text-embedding-3-small"
    # )

    # # 文档解析的结构化结果
    # print(result["type"], len(result["data"]))

    # print(result)

    # import json
    # save_path = f"test_output/P2023110253CN1_result.json"

    # with open(save_path, "w", encoding="utf-8") as f:
    #     json.dump(result, f, ensure_ascii=False, indent=2)

    # print(f"✅ 解析结果已保存至: {save_path}")
    # result = run_point_extraction(json_path='/app/converted_result.json', api_key="ae0550db705443238dd2595a58cd964c",
    #             api_base="https://ustc-law-gpt4-1.openai.azure.com",
    #             deployment_name="gpt-4o",
    #             save_dir="test_output/extract_results",
    #             max_workers=64,)
    # print(result)
    # with open("test_output/extract_results/P2023110253CN1_result.json", "w", encoding="utf-8") as f:
    #     json.dump(result, f, ensure_ascii=False, indent=2)
    
    
    process_folder(
        input_folder="/app/mma_eval_tmp/test_data/source_file",
        output_folder="/app/mma_eval_tmp/test_output/extract_results",
        tmp_folder="/app/mma_eval_tmp/test_output/tmp",
        LLM_Type="gpt-4o",
    )