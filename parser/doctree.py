"""
Unified DoctreeParser
支持 Markdown 文档与 Excel 表格数据的结构化解析 + 语义分块。
"""

import re
import json
from dataclasses import dataclass
from typing import Optional, List, Union
from openai import AzureOpenAI

CN_NUMS = "一二三四五六七八九十"


@dataclass
class Node:
    """树节点"""
    title: str
    level: int
    content: str
    children: List["Node"]

    def to_dict(self):
        """转为可序列化 dict"""
        return {
            "title": self.title,
            "level": self.level,
            "content": DoctreeParser.split_to_sentences(self.content),
            "children": [c.to_dict() for c in self.children],
        }

class DoctreeParser:
    """文档与表格统一结构化解析器"""

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_base: Optional[str] = None,
        api_version: str = "2024-02-15-preview",
        deployment_name: str = "gpt-4o",
    ):
        """初始化 LLM 与 Embedding 模型"""
        self.client = None
        if api_key and api_base:
            self.client = AzureOpenAI(
                api_key=api_key,
                api_version=api_version,
                azure_endpoint=api_base,
            )
        self.model = deployment_name

        self.embeddings = None

    @staticmethod
    def split_to_sentences(text: str) -> List[str]:
        """文本按标点分句"""
        text = text.strip()
        if not text:
            return []
        parts = re.split(r"[。！？\n\r]", text)
        return [p.strip() for p in parts if p.strip()]

    @staticmethod
    def extract_titles(text: str):
        """提取 Markdown 风格标题"""
        pat = re.compile(r"^(#+)\s*(.+)$", re.M)
        return [{"pos": m.start(), "raw": m.group(0), "title": m.group(2).strip()} for m in pat.finditer(text)]

    @staticmethod
    def split_by_titles(text: str, titles):
        """按标题切分正文（自动去掉标题行本身）"""
        docs = []
        for i, t in enumerate(titles):
            start = t["pos"]
            end = titles[i + 1]["pos"] if i + 1 < len(titles) else len(text)
            section = text[start:end].strip()

            lines = section.splitlines()
            if lines and lines[0].strip().startswith("#"):
                lines = lines[1:]
            clean_section = "\n".join(lines).strip()

            docs.append((t["title"], clean_section))
        return docs

    @staticmethod
    def rule_based_level(title: str):
        """规则判定层级"""
        s = title.strip()
        if s.startswith("#"):
            h = re.match(r"^(#+)", s)
            if h:
                return len(h.group(1)), "strong"
        if re.match(rf"^[{CN_NUMS}]+[、.]", s):
            return 1, "strong"
        if re.match(rf"^[（(][{CN_NUMS}]+[)）]", s):
            return 2, "strong"
        if re.match(r"^[（(]\d+[)）]", s):
            return 3, "strong"
        if re.match(r"^\d+\s*[、.)]", s):
            return 3, "strong"
        if re.match(r"^【.*】$", s):
            return 3, "strong"
        if re.search(r"(方案|发明点|性能测试|表征|制备|步骤)", s):
            return None, "weak"
        return None, "unknow"

    def llm_predict_level(self, title: str, prev_titles: list[str] = None) -> int:
        """
        使用 Azure OpenAI 判定标题层级。
        - 结合前文所有标题上下文，判断当前标题的层级。
        - 层级范围 1~6。
        """
        if not self.client:
            return 3

        prev_titles = prev_titles or []
        context_titles = "\n".join(f"- {t}" for t in prev_titles[-5:]) if prev_titles else "无"

        prompt = f"""
            你是一名文档结构分析助手。请根据前文标题的结构，判断当前标题的层级深度。
            层级从 1 到 3，1 表示最高级章节标题，3 表示最细节的小节标题，一般是 3，除非有明确正确。

            前文标题如下：
            {context_titles}

            当前标题：
            {title}

            请仅输出一个纯数字，例如 "3"。
        """

        try:
            resp = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt.strip()}],
                temperature=0,
            )
            text = resp.choices[0].message.content.strip()
            match = re.search(r"\d+", text)
            if match:
                lv = int(match.group())
                return max(1, min(lv, 6))
            else:
                return 3
        except Exception as e:
            return 3

    def build_doctree(self, sections):
        root = Node("ROOT", 0, "", [])
        stack = [root]
        for title, content, level in sections:
            if level - stack[-1].level > 1:
                level = stack[-1].level + 1
            node = Node(title, level, content, [])
            while stack and stack[-1].level >= level:
                stack.pop()
            stack[-1].children.append(node)
            stack.append(node)
        return root

    def parse_markdown(self, text: str):
        """Markdown 文本解析入口"""
        titles = self.extract_titles(text)
        sections_meta = []
        last_strong = 0
        prev_titles = []

        for i, t in enumerate(titles):
            title = t["title"]
            level, strength = self.rule_based_level(title)

            if strength == "strong":
                last_strong = level

            elif strength == "weak":
                level = min(last_strong + 1 if last_strong else 2, 3)

            else:
                level = self.llm_predict_level(title, prev_titles)
                last_strong = level

            sections_meta.append({"title": title, "level": level})

            prev_titles.append(title)

        split_sections = self.split_by_titles(text, titles)
        merged = [
            (title, sec, meta["level"])
            for (title, sec), meta in zip(split_sections, sections_meta)
        ]
        root = self.build_doctree(merged)
        result = [c.to_dict() for c in root.children]

        # if self.chunker:
        #     for node in result:
        #         self._semantic_chunk_node(node)
        return result

    def parse_table(self, excel_json: dict, header_row_index: int = 0, row_label_col_index: int = 0):
        """从 Excel JSON 提取每个单元格的三元组信息：(row_label, col_label, value)"""
        print("文本获取完成，开始解析...")

        triplets = []

        table_items = [x for x in excel_json.get("content", []) if x["type"] == "table"]
        if not table_items:
            raise ValueError("未检测到表格类型数据")

        table = table_items[0]["content"]
        headers = table.get("headers", [])
        data = table.get("data", [])

        if header_row_index < len(data):
            col_labels = data[header_row_index]
        else:
            col_labels = headers  # fallback
        col_labels = [str(c).strip() for c in col_labels]

        for i, row in enumerate(data):
            if not row or all(v == "" or v is None for v in row):
                continue

            row_label = str(row[row_label_col_index]).strip() if row_label_col_index < len(row) else f"Row{i}"
            for j, cell in enumerate(row):
                value = str(cell).strip()
                if not value or j == row_label_col_index:
                    continue
                col_label = col_labels[j] if j < len(col_labels) else f"Col{j}"
                if i != 0:
                    triplets.append({
                        "row_label": row_label,
                        "col_label": col_label,
                        "value": value,
                        "row_index": i,
                        "col_index": j
                    })

        return triplets



    def _semantic_chunk_node(self, node_dict):
        """对 level>=3 的节点执行语义分块"""
        if node_dict["level"] >= 3 and node_dict["content"]:
            text = "。".join(node_dict["content"])
            try:
                chunks = self.chunker.split_text(text)
                node_dict["content"] = chunks
            except Exception as e:
                print(f"语义分块失败 {node_dict['title']}: {e}")
        for child in node_dict["children"]:
            self._semantic_chunk_node(child)

    def serialize_with_titles(self, parsed_tree: List[dict]) -> List[dict]:
        """
        将树状结构序列化为扁平列表，每个文本块携带完整标题路径。
        输出示例：
        [
            {"title_path": "1. 技术方案 > 制备方法 > 步骤A", "text": "在温度为80°C下反应10分钟"},
            {"title_path": "1. 技术方案 > 性能测试", "text": "测试结果显示导电性提升20%"},
            ...
        ]
        """
        results = []

        def traverse(node: dict, path: List[str]):
            current_path = path + [node["title"]] if node["title"] != "ROOT" else path
            for sent in node.get("content", []):
                if sent.strip():
                    results.append({
                        "title_path": " > ".join(current_path),
                        "text": sent.strip()
                    })
            for child in node.get("children", []):
                traverse(child, current_path)

        for node in parsed_tree:
            traverse(node, [])

        return results

    def parse(self, data: Union[str, dict]):
        """自动识别输入类型（文本 or 表格JSON）"""
        if isinstance(data, dict):
            # print("检测到 Excel JSON 格式，执行表格解析…")
            return self.parse_table(data)
        elif isinstance(data, str):
            # print("检测到 Markdown 文本格式，执行文档树解析…")
            return self.parse_markdown(data)
        else:
            raise TypeError("输入必须为 str (Markdown 文本) 或 dict (Excel JSON)")

if __name__ == "__main__":
    parser = DoctreeParser(
        api_key="ae0550db705443238dd2595a58cd964c",
        api_base="https://ustc-law-gpt4-1.openai.azure.com",
        deployment_name="gpt-4o",
        # embedding_base="https://api.chatanywhere.tech",
        # embedding_api_key="sk-JAAXtIR3CJkPOSWxbcaCGeW9yad86WgjHOJHyF6D0EESq5f2",
        # embedding_model="text-embedding-3-small",
    )

    with open("tmp/test.md", "r", encoding="utf-8") as f:
        text = f.read()
    result = parser.parse(text)

    print(result)

    import json
    # from parse_excel import analyze_excel
    # analyze_excel("tmp/test.xlsx")

    with open("tmp/chunk.json", "r", encoding="utf-8") as f:
        excel_json = json.load(f)
    triplets = parser.parse(excel_json)

    print(json.dumps(triplets[:3], ensure_ascii=False, indent=2))
