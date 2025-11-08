from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional, Literal
import pandas as pd
import io
import uuid

# Initialize FastAPI app
app = FastAPI(title="ETL as Endpoints (Mini Demo)", version="1.0.0")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

# Simple in-memory "staging area" token -> pandas.DataFrame
TOKENS: Dict[str, pd.DataFrame] = {}

# Helper function
def ensure_columns(df: pd.DataFrame, required: List[str]):
    """
    Check if required columns exist in the DataFrame.
    Raise HTTPException if missing.
    """
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Missing required columns: {missing}"
        )

# Root endpoint
@app.get("/")
def root():
    return {
        "message": "ETL Endpoints: /extract/csv, /extract/json, /transform, /load",
        "docs": "/docs"
    }


# ============ EXTRACT ============

@app.post("/extract_csv/")
async def extract_csv(
    file: UploadFile = File(..., description="CSV file"),
    required_cols: Optional[str] = Form(None, description="Comma-separated required column names (optional)")
):
    """
    Accepts CSV via multipart upload (Swagger UI), optionally validates columns.
    Returns a token for chaining in a small pipeline.
    """
    try:
        content = await file.read()
        df = pd.read_csv(io.BytesIO(content))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Unable to read CSV: {e}")
    
    if required_cols:
        ensure_columns(df, [c.strip() for c in required_cols.split(",") if c.strip()])
    
    token = str(uuid.uuid4())
    TOKENS[token] = df
    
    return {
        "message": "CSV extracted",
        "token": token,
        "rows": len(df),
        "columns": list(df.columns),
        "preview_head": df.head().to_dict(orient="records")
    }


@app.post("/extract_json/")
async def extract_json(
    file: UploadFile = File(..., description="JSON file"),
    required_cols: Optional[str] = Form(None, description="Comma-separated required column names (optional)")
):
    """
    Similar to extract_csv but for JSON files.
    """
    try:
        content = await file.read()
        df = pd.read_json(io.BytesIO(content))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Unable to read JSON: {e}")
    
    if required_cols:
        ensure_columns(df, [c.strip() for c in required_cols.split(",") if c.strip()])
    
    token = str(uuid.uuid4())
    TOKENS[token] = df
    
    return {
        "message": "JSON extracted",
        "token": token,
        "rows": len(df),
        "columns": list(df.columns),
        "preview_head": df.head().to_dict(orient="records")
    }


# ============ TRANSFORM ============

class FilterClause(BaseModel):
    col: str
    op: Literal["==", "!=", ">", "<", ">=", "<=", "contains", "isin"]
    value: Any


class TransformBody(BaseModel):
    token: str = Field(..., description="Token returned from /extract")
    select: Optional[List[str]] = Field(None, description="Columns to keep, in order")
    rename: Optional[Dict[str, str]] = Field(None, description="Old->new column names")
    filters: Optional[List[FilterClause]] = Field(None, description="Filter conditions applied sequentially")


@app.post("/transform")
def transform(body: TransformBody):
    """
    Transform the DataFrame stored in TOKENS.
    """
    if body.token not in TOKENS:
        raise HTTPException(status_code=400, detail="Invalid or expired token")
    
    df = TOKENS[body.token].copy()
    
    # Select columns
    if body.select:
        ensure_columns(df, body.select)
        df = df[body.select]
    
    # Rename columns
    if body.rename:
        df = df.rename(columns=body.rename)
    
    # Apply filters
    if body.filters:
        for f in body.filters:
            if f.col not in df.columns:
                raise HTTPException(status_code=400, detail=f"Column {f.col} not found")
            
            series = df[f.col]
            
            if f.op == "==":
                mask = series == f.value
            elif f.op == "!=":
                mask = series != f.value
            elif f.op == ">":
                mask = series > f.value
            elif f.op == "<":
                mask = series < f.value
            elif f.op == ">=":
                mask = series >= f.value
            elif f.op == "<=":
                mask = series <= f.value
            elif f.op == "contains":
                if not isinstance(f.value, list):
                    raise HTTPException(status_code=400, detail="'contains' requires a list as value")
                mask = series.astype(str).str.contains(str(f.value), case=False, na=False)
            elif f.op == "isin":
                if not isinstance(f.value, list):
                    raise HTTPException(status_code=400, detail="'isin' requires a list as value")
                mask = series.astype(str).isin([str(v) for v in f.value])
            else:
                raise HTTPException(status_code=400, detail=f"Unsupported op: {f.op}")
            
            df = df[mask]
    
    # Persist transformed frame back to token (chaining)
    TOKENS[body.token] = df
    
    return {
        "message": "Transformed",
        "token": body.token,
        "rows": len(df),
        "columns": list(df.columns),
        "preview_head": df.head().to_dict(orient="records")
    }


# ============ LOAD ============

class LoadBody(BaseModel):
    token: str
    table_name: str
    db_path: str = "etl.db"
    if_exists: Literal["replace", "append", "fail"] = "replace"


@app.post("/load")
def load(body: LoadBody):
    """
    Load the DataFrame to a SQLite database.
    """
    if body.token not in TOKENS:
        raise HTTPException(status_code=400, detail="Invalid or expired token")
    
    df = TOKENS[body.token]
    if len(df) == 0:
        raise HTTPException(status_code=400, detail="No rows to load")
    
    # Simulate database load (using SQLite for demo)
    import sqlite3
    conn = sqlite3.connect(body.db_path)
    df.to_sql(body.table_name, conn, if_exists=body.if_exists, index=False)
    conn.close()
    
    return {
        "message": f"Loaded {len(df)} rows to {body.table_name} in {body.db_path}",
        "table": body.table_name,
        "rows": len(df)
    }
