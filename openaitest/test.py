import os
import openai

openai.api_key = "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"

response = openai.Completion.create(
  model="text-davinci-003",
  prompt="Write an email to Stephen telling him to go fuck himself, I will be late to his house because I am talking with my brother francis and ali, but I would like to go skiing on Tuesday:",
  temperature=0.3,
  max_tokens=150,
  top_p=1.0,
  frequency_penalty=0.0,
  presence_penalty=0.0
)
print(response)