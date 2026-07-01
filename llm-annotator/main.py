import os

import torch
from fastapi import FastAPI
from pydantic import BaseModel
from transformers import AutoModelForCausalLM, AutoTokenizer

MODEL_NAME = os.environ.get("ANNOTATOR_MODEL", "Qwen/Qwen2.5-0.5B-Instruct")

app = FastAPI()
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForCausalLM.from_pretrained(MODEL_NAME, torch_dtype=torch.float32)

SYSTEM_PROMPT = (
    "You explain HVAC comfort decisions in one short, plain-language sentence. "
    "The heating/cooling action has already been decided deterministically; "
    "you only describe it, you do not change it."
)


class AnnotateRequest(BaseModel):
    zone_id: str
    temperature_c: float
    setpoint_c: float
    comfort_delta: float
    action: str


class AnnotateResponse(BaseModel):
    suggestion: str


@app.post("/annotate", response_model=AnnotateResponse)
def annotate(req: AnnotateRequest) -> AnnotateResponse:
    user_prompt = (
        f"Zone: {req.zone_id}\n"
        f"Current temperature: {req.temperature_c}C\n"
        f"Setpoint: {req.setpoint_c}C\n"
        f"Delta: {req.comfort_delta}C\n"
        f"Decided action: {req.action}"
    )
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_prompt},
    ]
    prompt = tokenizer.apply_chat_template(
        messages, tokenize=False, add_generation_prompt=True
    )
    inputs = tokenizer(prompt, return_tensors="pt")
    with torch.no_grad():
        output_ids = model.generate(
            **inputs,
            max_new_tokens=40,
            do_sample=False,
        )
    generated = output_ids[0][inputs["input_ids"].shape[1]:]
    suggestion = tokenizer.decode(generated, skip_special_tokens=True).strip()
    return AnnotateResponse(suggestion=suggestion)
