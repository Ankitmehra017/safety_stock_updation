# Generac Safety Stock Update System

End-to-end system for estimating and approving safety stock updates using Databricks (local Delta Lake), MLflow, and Streamlit.

## Architecture

```
Bronze (raw dummy data)
  └─► Silver (cleaned + weekly aggregation)
        └─► Gold (ML features)
              └─► MLflow model training
                    └─► Batch scoring → SS Recommendations
                              └─► Streamlit App
                                    ├── Buyer Dashboard     (review & submit)
                                    ├── Genie QA            (Claude text-to-SQL)
                                    └── Manager Approval    (approve / reject)
```

## Quick Start

### 1. Install dependencies

```bash
cd safety_stock_updation
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env and add your ANTHROPIC_API_KEY
```

### 3. Run the full pipeline

```bash
python run_pipeline.py
```

Or run notebooks individually:

```bash
python notebooks/01_create_dummy_data.py    # Seed bronze Delta tables
python notebooks/02_medallion_pipeline.py   # Bronze → Silver → Gold
python notebooks/03_train_model.py          # Train + register ML model
python notebooks/04_batch_scoring.py        # Score + write recommendations
```

### 4. Launch the Streamlit app

```bash
streamlit run app/streamlit_app.py
```

## File Structure

```
safety_stock_updation/
├── notebooks/
│   ├── 01_create_dummy_data.py      # Dummy data: 100 materials, 2yr demand
│   ├── 02_medallion_pipeline.py     # Bronze → Silver → Gold transforms
│   ├── 03_train_model.py            # RandomForest + MLflow training
│   └── 04_batch_scoring.py          # SHAP explanations + recommendations
├── app/
│   ├── streamlit_app.py             # Entry point (run this)
│   ├── pages/
│   │   ├── 1_Buyer_Dashboard.py     # SS comparison + submit approval
│   │   ├── 2_Genie_QA.py           # Claude text-to-SQL QA agent
│   │   └── 3_Manager_Approval.py   # Approve / reject workflow
│   └── backend/
│       ├── db.py                    # Delta table read functions
│       ├── genie_agent.py           # Claude-powered SQL generation + DuckDB execution
│       └── approval.py              # Approval request CRUD
├── config.py                        # Central config (reads .env)
├── run_pipeline.py                  # Convenience: run all notebooks
├── requirements.txt
└── .env.example
```

## Data Model

### Bronze
| Table | Description |
|-------|-------------|
| `materials` | Material master (100 materials, 3 plants) |
| `historical_demand` | Daily demand, 2 years |
| `lead_times` | PO-level lead times |
| `buyers` | 5 buyers with manager mapping |
| `current_safety_stock` | Current SS values |

### Gold Features (ML input)
`demand_mean`, `demand_std`, `demand_cv`, `lead_time_mean`, `lead_time_std`, `service_level_z`, `abc_class_encoded`, `category_encoded`

### Serving
| Table | Description |
|-------|-------------|
| `ss_recommendations` | Model output: current SS, new SS, % change, SHAP drivers |
| `approval_requests` | Workflow state: pending / approved / rejected |

## Safety Stock Formula (ground truth for training)

```
SS = Z × √(L × σ_d² + d̄² × σ_L²)

Z    = service level Z-score (1.28 / 1.645 / 2.326)
L    = mean lead time (weeks)
σ_d  = weekly demand std dev
d̄    = mean weekly demand
σ_L  = lead time std dev (weeks)
```

## Genie QA (without Claude API)

Genie falls back to keyword-based SQL when `ANTHROPIC_API_KEY` is not set. Add your key to `.env` for full natural-language capability.

## Deploying to Databricks

The notebooks are plain Python — upload them to a Databricks workspace and run on any cluster. Change `DELTA_TABLE_PATH` in `.env` to your Unity Catalog path (e.g. `dbfs:/user/hive/warehouse/`).
