from openai import OpenAI
from dotenv import load_dotenv
import os

class GPTToolExtractor:
    def __init__(self, input_list):
        load_dotenv()
        self.input_list = input_list
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.result = self.gpt_tools()

    def gpt_tools(self):
        print("GPT started")
        prompt = f"""
From the given list, extract only the names of current, widely used technical tools, programming languages, and software (e.g., Python, AWS, Docker). 
Exclude roles, industries, methodologies, and vague categories. 
Be strict and critical: include only relevant and actively used technologies; skip deprecated or irrelevant terms. 
Return the result as a single comma-separated list with no extra text. 
If no valid items are found, dont write anything , leave it blank.
        List: {', '.join(self.input_list)}
        """

        response = self.client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "user", "content": prompt}
            ],
            temperature=0.2,
            max_tokens=150
        )

        return response.choices[0].message.content.strip()
