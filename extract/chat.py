import openai
import requests
import logging
logger = logging.getLogger(__name__)

from openai import AzureOpenAI, OpenAI
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=1, max=10),
    retry=retry_if_exception_type((openai.OpenAIError, requests.exceptions.RequestException)),
    reraise=True
)
def chat_infer(text: str, LLM_Type: str) -> str:
    """
    调用 LLM 进行推理，并自动重试
    
    Args:
        text: 输入文本
        model: 使用的模型名称
        
    Returns:
        模型响应文本
    Raises:
        ModelProcessingError: 如果达到最大重试次数仍失败
    """
    ## TODO ##
    ## 根据不同的模型，调用不同的LLM
    if LLM_Type == "gpt-4o":
        client = AzureOpenAI(
            api_key="ae0550db705443238dd2595a58cd964c",
            api_version="2024-02-15-preview",
            azure_endpoint="https://ustc-law-gpt4-1.openai.azure.com"
        )
        model = "gpt-4o"
        messages = [
            {"role": "user", "content": text}
        ]
        completion = client.chat.completions.create(
            model = model,
            messages = messages
        )
        try:
            result = completion.choices[0].message.content
            # print(result)
            if result is None:
                return ""
            return result
        except Exception as e:
            logger.error(f"调用 {model} 模型失败: {e}")
            return ""
        

    elif LLM_Type == "deepseek":
        client = OpenAI(
            base_url = "http://oneapi.catl.com/v1",
            api_key = "sk-"
        )
        model = "DeepSeek-V3-0324"
        messages = [
            {"role": "user", "content": text}
        ]
        completion = client.chat.completions.create(
            model = model,
            messages = messages
        )
        try:
            result = completion.choices[0].message.content
            print(result)
            if result is None:
                return ""
            return result
        except Exception as e:
            logger.error(f"调用 {model} 模型失败: {e}")
            return ""
        

    elif LLM_Type == "Qwen2.5":
        pass
    elif LLM_Type == "Qwen3":
        pass
    elif LLM_Type == "glm4":
        pass


    try:
        response = client.chat.completions.create(
            messages=[{"role": "user", "content": text}],
            model=model,
            temperature=0
        )
        content = response.choices[0].message.content
        if content is None:
            return ""
        return content
    except Exception as e:
        logger.error(f"调用 GPT 模型失败: {e}")
        return ""
