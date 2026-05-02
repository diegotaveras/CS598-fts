# **Semantica**
*A semantic-embedding overlay used to optimize search in P2P systems*  

📄 **Paper**: [arXiv:2502.10151](https://arxiv.org/abs/2502.10151)  

## 🚧 Work in Progress

This repository implements the **tree construction** and **expansion-round algorithms** from the paper. The main script, `main.py`, handles the execution of these algorithms.

---

## ⚙️ Configuration Parameters
You can customize several parameters before running the script:

### **Dataset Requirements**
- **`nbr_train_recs_list`** – Number of records required per user for the *training set*.
- **`nbr_test_records_list`** – Minimum number of records required per user for the *test set*.
- **Note**: Only users with at least `nbr_train_recs_list + nbr_test_records_list` records will be tested. However, the tree may still contain users with fewer records based on the `small_user_filter_for_test_set` setting.

### **Tree Construction Parameters**
- **`record_limit_per_leafnode_list`** – Threshold number of users per leaf node before it becomes a split-node.
- **`small_user_filter_for_test_set`** *(Boolean)* – 
  - `True`: Only include users with sufficient data in the tree.  
  - `False`: Include all users but only test those with enough data.

### **Querying Strategies**
- **`perform_query_options`** *(Default: `None`)* – Defines how queries are processed in the tree. Options:
  - `"Query-as-Node"` – Places the query in the tree and searches for nearest neighbors.
  - `"Closest-Users"` – Queries the closest users to the query based on the querying-user's known-users.
  - `"Closest-User-Chain"` – Uses query forwarding to the neighbor most similar to the query.

  - For the moment, all methods send a hardcoded 50 total queries to the network.
### **Search and Expansion Parameters**
- **`N_list`** *(Default: `[50]`)* – Number of closest users to compare.
- **`M_multipliers`** *(Default: `[1]`)* – Determines how many users a query will initially contact before selecting the `N` closest users based on cosine similarity.
- **`DELTA_list`** *(Default: `[5e-4]`)* – As described in the paper.
- **`expansion_rounds_list`** *(Default: `[0]`)* – Number of expansion rounds, as described in the paper.

### **Miscellaneous**
- **`consider_contacts_options`** *(Default: `[False]`)* – (Ignore this for now.)
- **`plot_results`** *(Default: `True`)* – Enables visualization of results.

---

## 📥 Preparing Data

Create a `dataset` subfolder in the main folder and place in it `data.csv`, `doc.csv` and `query.csv` files from AOL4PS.

The algorithm requires both queries and documents to be **vector embeddings** (such as with an LLM).  
To generate these embeddings, run `embed_BERT.py` from the `Semantica/` directory:

```bash
python embed_BERT.py            # both docs and queries
python embed_BERT.py docs       # only docs
python embed_BERT.py queries    # only queries
```

This produces `embedded_docs_BERT_df.pkl` and `embedded_queries_df_BERT.pkl`. *(The doc-pickle path is currently hardcoded in `embedding/loader.py`; if you change it, update that file as well.)*

Once the embeddings are generated, you can proceed with running the main algorithm.

---

## 🏗️ Running the Code

From inside the `Semantica/` directory, run:

```bash
python main.py
```
