import pandas as pd
import torch
from transformers import BertTokenizer, BertModel


def encode_csv_to_pickle(
    input_csv,
    text_col,
    output_pkl,
    log_path='progress_log.txt',
    log_every=100,
    model_name='bert-base-uncased',
    sep='\t',
    max_length=512,
):
    tokenizer = BertTokenizer.from_pretrained(model_name)
    model = BertModel.from_pretrained(model_name)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = model.to(device)

    df = pd.read_csv(input_csv, sep=sep)

    with open(log_path, 'w') as f:
        f.write("Progress Log Initialized\n")

    counter = {"n": 0}

    def encode(text):
        counter["n"] += 1
        if counter["n"] % log_every == 0:
            with open(log_path, 'a') as f:
                f.write(f"{counter['n']} texts encoded.\n")
        inputs = tokenizer(text, return_tensors="pt", truncation=True,
                           padding="max_length", max_length=max_length)
        inputs = {k: v.to(device) for k, v in inputs.items()}
        with torch.no_grad():
            outputs = model(**inputs)
        return outputs.last_hidden_state.mean(dim=1).squeeze().cpu().numpy()

    df["embedding"] = df[text_col].apply(encode)
    df.to_pickle(output_pkl)
