import os
from typing import List, Optional
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, validator
from datetime import datetime
import random

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class RunRequest(BaseModel):
    from_date: str = Field(..., description="Report Date in DDMMMYYYY format")
    to_date: Optional[str] = Field(None, description="Previous Date in DDMMMYYYY format")
    lines: str = Field(..., description="Comma-separated LCR line numbers, e.g., '6,17'")
    country: str = Field(..., description="ISO country code, e.g., 'SG'")

    @validator("from_date")
    def validate_from_date(cls, v):
        try:
            datetime.strptime(v.upper(), "%d%b%Y")
        except Exception:
            raise ValueError("from_date must be DDMMMYYYY, e.g., 31AUG2019")
        return v.upper()

    @validator("to_date")
    def validate_to_date(cls, v):
        if v in (None, "",):
            return None
        try:
            datetime.strptime(v.upper(), "%d%b%Y")
        except Exception:
            raise ValueError("to_date must be DDMMMYYYY, e.g., 31JUL2019")
        return v.upper()

    @validator("lines")
    def validate_lines(cls, v):
        try:
            parts = [p.strip() for p in v.split(',') if p.strip()]
            if not parts:
                raise ValueError
            _ = [int(p) for p in parts]
        except Exception:
            raise ValueError("lines must be comma-separated integers, e.g., '6,17'")
        return ",".join(parts)

    @validator("country")
    def validate_country(cls, v):
        v = v.strip().upper()
        if not v or len(v) not in (2, 3):
            raise ValueError("country must be ISO code like 'SG' or 'SGP'")
        return v


@app.get("/")
def read_root():
    return {"message": "Hello from FastAPI Backend!"}


@app.get("/api/hello")
def hello():
    return {"message": "Hello from the backend API!"}


@app.get("/test")
def test_database():
    """Test endpoint to check if database is available and accessible"""
    response = {
        "backend": "✅ Running",
        "database": "❌ Not Available",
        "database_url": None,
        "database_name": None,
        "connection_status": "Not Connected",
        "collections": []
    }

    try:
        # Try to import database module
        from database import db

        if db is not None:
            response["database"] = "✅ Available"
            response["database_url"] = "✅ Configured"
            response["database_name"] = db.name if hasattr(db, 'name') else "✅ Connected"
            response["connection_status"] = "Connected"

            # Try to list collections to verify connectivity
            try:
                collections = db.list_collection_names()
                response["collections"] = collections[:10]  # Show first 10 collections
                response["database"] = "✅ Connected & Working"
            except Exception as e:
                response["database"] = f"⚠️  Connected but Error: {str(e)[:50]}"
        else:
            response["database"] = "⚠️  Available but not initialized"

    except ImportError:
        response["database"] = "❌ Database module not found (run enable-database first)"
    except Exception as e:
        response["database"] = f"❌ Error: {str(e)[:50]}"

    # Check environment variables
    import os
    response["database_url"] = "✅ Set" if os.getenv("DATABASE_URL") else "❌ Not Set"
    response["database_name"] = "✅ Set" if os.getenv("DATABASE_NAME") else "❌ Not Set"

    return response


@app.post("/run")
def run_job(payload: RunRequest):
    """
    This endpoint receives parameters from the UI and (in a real deployment)
    would invoke SAS Viya JES to run a .sas job with those parameters and then
    read the resulting SAS dataset to return as JSON rows for display.

    In this sandbox, we simulate the dataset output so the UI can be developed.
    """
    try:
        # Parse inputs
        report_date = datetime.strptime(payload.from_date, "%d%b%Y")
        prev_date = (
            datetime.strptime(payload.to_date, "%d%b%Y") if payload.to_date else None
        )
        line_numbers = [int(x.strip()) for x in payload.lines.split(",") if x.strip()]
        country = payload.country

        # Simulate rows resembling a SAS dataset you'd print
        random.seed(42)
        rows: List[dict] = []
        for ln in line_numbers:
            value_today = round(random.uniform(1000, 5000) * (1 + ln / 100), 2)
            value_prev = (
                round(value_today * random.uniform(0.9, 1.1), 2) if prev_date else None
            )
            rows.append(
                {
                    "COUNTRY": country,
                    "LINE": ln,
                    "REPORT_DATE": report_date.strftime("%d%b%Y"),
                    "PREV_DATE": prev_date.strftime("%d%b%Y") if prev_date else "",
                    "VALUE": value_today,
                    "PREV_VALUE": value_prev if value_prev is not None else "",
                    "DELTA": round(value_today - value_prev, 2) if value_prev else "",
                }
            )

        return {"rows": rows}

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
