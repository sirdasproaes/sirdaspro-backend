"""
SIRDASProAES Backend — FastAPI
Provides REST API for inventory, sales, expenses, settings, and AI assistant.

Run locally:
    uvicorn main:app --reload --port 8000
"""

import json
import os
from contextlib import asynccontextmanager
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from database import (
    execute,
    get_connection,
    init_db,
    parse_items,
    reset_sequences,
    row_to_dict,
    rows_to_list,
)

load_dotenv()

ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")


# ---------------------------------------------------------------------------
# App lifecycle
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="SIRDASProAES API",
    description="Backend for the SIRDASProAES stamp-shop management dashboard.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class ProductCreate(BaseModel):
    code: str
    name: str
    cat: str = ""
    brand: str = ""
    size: str = ""
    ink: str = ""
    price: float = 0
    cost: float = 0
    stock: int = 0
    notes: str = ""
    current_stock_price: float = 0
    avg_stock_price: float = 0


class ProductUpdate(BaseModel):
    name: Optional[str] = None
    cat: Optional[str] = None
    brand: Optional[str] = None
    size: Optional[str] = None
    ink: Optional[str] = None
    price: Optional[float] = None
    cost: Optional[float] = None
    stock: Optional[int] = None
    notes: Optional[str] = None
    current_stock_price: Optional[float] = None
    avg_stock_price: Optional[float] = None


class RestockRequest(BaseModel):
    qty: int = Field(..., gt=0)
    unit_price: float = Field(..., ge=0)


class SaleItem(BaseModel):
    product_id: int = 0
    ref: str = ""
    name: str
    qty: int = Field(..., gt=0)
    unit_price: float = Field(..., ge=0)
    subtotal: float = 0


class SaleCreate(BaseModel):
    num: Optional[int] = None
    date: str
    client: str = ""
    payment: str = "Espèces"
    items: list[SaleItem]
    total: Optional[float] = None


class ExpenseCreate(BaseModel):
    date: str
    cat: str
    description: str = ""
    amount: float = Field(..., ge=0)


class SettingsUpdate(BaseModel):
    shop_name: Optional[str] = None
    currency: Optional[str] = None
    low_stock: Optional[int] = None
    lang: Optional[str] = None


class BulkProductsRequest(BaseModel):
    products: list[ProductCreate]


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _next_sale_num(conn) -> int:
    row = execute(conn, "SELECT MAX(num) AS m FROM sales").fetchone()
    return (row["m"] or 1000) + 1


def _recalc_avg_price(current_stock: int, current_price: float,
                      incoming_qty: int, incoming_price: float) -> float:
    total_qty = current_stock + incoming_qty
    if total_qty == 0:
        return incoming_price
    return ((current_stock * current_price) + (incoming_qty * incoming_price)) / total_qty


# ---------------------------------------------------------------------------
# Products
# ---------------------------------------------------------------------------

@app.get("/api/products", tags=["Products"])
def list_products(
    cat: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    low_stock: Optional[bool] = Query(None),
):
    conn = get_connection()
    sql = "SELECT * FROM products WHERE 1=1"
    params: list = []

    if cat:
        sql += " AND cat = ?"
        params.append(cat)
    if search:
        sql += " AND (name ILIKE ? OR code ILIKE ?)"
        params += [f"%{search}%", f"%{search}%"]

    rows = rows_to_list(execute(conn, sql, params).fetchall())

    if low_stock is not None:
        threshold = execute(conn, "SELECT low_stock FROM settings WHERE id=1").fetchone()
        t = threshold["low_stock"] if threshold else 5
        if low_stock:
            rows = [r for r in rows if r["stock"] <= t]

    conn.close()
    return rows


@app.get("/api/products/{product_id}", tags=["Products"])
def get_product(product_id: int):
    conn = get_connection()
    row = execute(conn, "SELECT * FROM products WHERE id=?", (product_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Product not found")
    return row_to_dict(row)


@app.post("/api/products", status_code=201, tags=["Products"])
def create_product(body: ProductCreate):
    conn = get_connection()
    try:
        with conn:
            cur = execute(
                conn,
                """INSERT INTO products
                   (code,name,cat,brand,size,ink,price,cost,stock,notes,
                    current_stock_price,avg_stock_price)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?) RETURNING id""",
                (body.code, body.name, body.cat, body.brand, body.size, body.ink,
                 body.price, body.cost, body.stock, body.notes,
                 body.current_stock_price, body.avg_stock_price),
            )
            new_id = cur.fetchone()["id"]
    except Exception as exc:
        conn.close()
        if "unique" in str(exc).lower():
            raise HTTPException(409, f"Product with code '{body.code}' already exists")
        raise HTTPException(500, str(exc))
    row = execute(conn, "SELECT * FROM products WHERE id=?", (new_id,)).fetchone()
    conn.close()
    return row_to_dict(row)


@app.post("/api/products/bulk", status_code=201, tags=["Products"])
def bulk_upsert_products(body: BulkProductsRequest):
    conn = get_connection()
    with conn:
        for p in body.products:
            execute(
                conn,
                """INSERT INTO products
                   (code,name,cat,brand,size,ink,price,cost,stock,notes,
                    current_stock_price,avg_stock_price)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(code) DO UPDATE SET
                     name=EXCLUDED.name, cat=EXCLUDED.cat, brand=EXCLUDED.brand,
                     size=EXCLUDED.size, ink=EXCLUDED.ink,
                     price=EXCLUDED.price, cost=EXCLUDED.cost, stock=EXCLUDED.stock,
                     notes=EXCLUDED.notes,
                     current_stock_price=EXCLUDED.current_stock_price,
                     avg_stock_price=EXCLUDED.avg_stock_price""",
                (p.code, p.name, p.cat, p.brand, p.size, p.ink,
                 p.price, p.cost, p.stock, p.notes,
                 p.current_stock_price, p.avg_stock_price),
            )
    conn.close()
    return {"message": f"{len(body.products)} products upserted"}


@app.put("/api/products/{product_id}", tags=["Products"])
def update_product(product_id: int, body: ProductUpdate):
    conn = get_connection()
    existing = execute(conn, "SELECT * FROM products WHERE id=?", (product_id,)).fetchone()
    if not existing:
        conn.close()
        raise HTTPException(404, "Product not found")

    fields = {k: v for k, v in body.model_dump().items() if v is not None}
    if not fields:
        conn.close()
        return row_to_dict(existing)

    set_clause = ", ".join(f"{k}=%s" for k in fields)
    values = list(fields.values()) + [product_id]
    with conn:
        conn.cursor().execute(f"UPDATE products SET {set_clause} WHERE id=%s", values)
    row = execute(conn, "SELECT * FROM products WHERE id=?", (product_id,)).fetchone()
    conn.close()
    return row_to_dict(row)


@app.delete("/api/products/{product_id}", status_code=204, tags=["Products"])
def delete_product(product_id: int):
    conn = get_connection()
    row = execute(conn, "SELECT id FROM products WHERE id=?", (product_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Product not found")
    with conn:
        execute(conn, "DELETE FROM products WHERE id=?", (product_id,))
    conn.close()


@app.post("/api/products/{product_id}/restock", tags=["Products"])
def restock_product(product_id: int, body: RestockRequest):
    conn = get_connection()
    row = execute(conn, "SELECT * FROM products WHERE id=?", (product_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Product not found")

    p = row_to_dict(row)
    new_avg = _recalc_avg_price(
        p["stock"], p["current_stock_price"], body.qty, body.unit_price
    )
    new_stock = p["stock"] + body.qty

    with conn:
        execute(
            conn,
            "UPDATE products SET stock=?, current_stock_price=?, avg_stock_price=?, cost=? WHERE id=?",
            (new_stock, body.unit_price, new_avg, body.unit_price, product_id),
        )
    row = execute(conn, "SELECT * FROM products WHERE id=?", (product_id,)).fetchone()
    conn.close()
    return row_to_dict(row)


# ---------------------------------------------------------------------------
# Sales
# ---------------------------------------------------------------------------

@app.get("/api/sales", tags=["Sales"])
def list_sales(
    search: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
):
    conn = get_connection()
    rows = rows_to_list(execute(conn, "SELECT * FROM sales ORDER BY id DESC").fetchall())
    conn.close()

    rows = [parse_items(r) for r in rows]

    if search:
        s = search.lower()
        rows = [
            r for r in rows
            if s in r.get("client", "").lower()
            or s in str(r.get("num", ""))
            or any(s in item.get("name", "").lower() for item in r.get("items", []))
        ]
    if date_from:
        rows = [r for r in rows if r["date"] >= date_from]
    if date_to:
        rows = [r for r in rows if r["date"] <= date_to]

    return rows


@app.get("/api/sales/{sale_id}", tags=["Sales"])
def get_sale(sale_id: int):
    conn = get_connection()
    row = execute(conn, "SELECT * FROM sales WHERE id=?", (sale_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Sale not found")
    return parse_items(row_to_dict(row))


@app.post("/api/sales", status_code=201, tags=["Sales"])
def create_sale(body: SaleCreate):
    conn = get_connection()
    items = [i.model_dump() for i in body.items]
    for item in items:
        item["subtotal"] = item["qty"] * item["unit_price"]
    total = body.total if body.total is not None else sum(i["subtotal"] for i in items)
    num = body.num if body.num is not None else _next_sale_num(conn)

    with conn:
        for item in items:
            if item.get("product_id"):
                execute(
                    conn,
                    "UPDATE products SET stock = GREATEST(0, stock - ?) WHERE id=?",
                    (item["qty"], item["product_id"]),
                )
        cur = execute(
            conn,
            "INSERT INTO sales (num,date,client,payment,items,total) VALUES (?,?,?,?,?,?) RETURNING id",
            (num, body.date, body.client, body.payment, json.dumps(items), total),
        )
        new_id = cur.fetchone()["id"]

    row = execute(conn, "SELECT * FROM sales WHERE id=?", (new_id,)).fetchone()
    conn.close()
    return parse_items(row_to_dict(row))


@app.delete("/api/sales/{sale_id}", status_code=204, tags=["Sales"])
def delete_sale(sale_id: int):
    conn = get_connection()
    row = execute(conn, "SELECT * FROM sales WHERE id=?", (sale_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Sale not found")

    sale = parse_items(row_to_dict(row))
    with conn:
        for item in sale.get("items", []):
            if item.get("product_id"):
                execute(
                    conn,
                    "UPDATE products SET stock = stock + ? WHERE id=?",
                    (item["qty"], item["product_id"]),
                )
        execute(conn, "DELETE FROM sales WHERE id=?", (sale_id,))
    conn.close()


# ---------------------------------------------------------------------------
# Expenses
# ---------------------------------------------------------------------------

@app.get("/api/expenses", tags=["Expenses"])
def list_expenses(
    cat: Optional[str] = Query(None),
    month: Optional[str] = Query(None),
):
    conn = get_connection()
    rows = rows_to_list(execute(conn, "SELECT * FROM expenses ORDER BY date DESC").fetchall())
    conn.close()

    if cat:
        rows = [r for r in rows if r["cat"] == cat]
    if month:
        rows = [r for r in rows if r["date"].startswith(month) or month in r["date"]]

    return rows


@app.get("/api/expenses/{expense_id}", tags=["Expenses"])
def get_expense(expense_id: int):
    conn = get_connection()
    row = execute(conn, "SELECT * FROM expenses WHERE id=?", (expense_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Expense not found")
    return row_to_dict(row)


@app.post("/api/expenses", status_code=201, tags=["Expenses"])
def create_expense(body: ExpenseCreate):
    conn = get_connection()
    with conn:
        cur = execute(
            conn,
            "INSERT INTO expenses (date,cat,description,amount) VALUES (?,?,?,?) RETURNING id",
            (body.date, body.cat, body.description, body.amount),
        )
        new_id = cur.fetchone()["id"]
    row = execute(conn, "SELECT * FROM expenses WHERE id=?", (new_id,)).fetchone()
    conn.close()
    return row_to_dict(row)


@app.delete("/api/expenses/{expense_id}", status_code=204, tags=["Expenses"])
def delete_expense(expense_id: int):
    conn = get_connection()
    row = execute(conn, "SELECT id FROM expenses WHERE id=?", (expense_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Expense not found")
    with conn:
        execute(conn, "DELETE FROM expenses WHERE id=?", (expense_id,))
    conn.close()


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

@app.get("/api/settings", tags=["Settings"])
def get_settings():
    conn = get_connection()
    row = execute(conn, "SELECT * FROM settings WHERE id=1").fetchone()
    conn.close()
    return row_to_dict(row)


@app.put("/api/settings", tags=["Settings"])
def update_settings(body: SettingsUpdate):
    conn = get_connection()
    fields = {k: v for k, v in body.model_dump().items() if v is not None}
    if fields:
        set_clause = ", ".join(f"{k}=%s" for k in fields)
        with conn:
            conn.cursor().execute(
                f"UPDATE settings SET {set_clause} WHERE id=1", list(fields.values())
            )
    row = execute(conn, "SELECT * FROM settings WHERE id=1").fetchone()
    conn.close()
    return row_to_dict(row)


# ---------------------------------------------------------------------------
# Dashboard stats
# ---------------------------------------------------------------------------

@app.get("/api/dashboard/stats", tags=["Dashboard"])
def dashboard_stats():
    conn = get_connection()

    total_revenue = execute(conn, "SELECT COALESCE(SUM(total),0) AS t FROM sales").fetchone()["t"]
    total_expenses = execute(conn, "SELECT COALESCE(SUM(amount),0) AS t FROM expenses").fetchone()["t"]
    product_count = execute(conn, "SELECT COUNT(*) AS c FROM products").fetchone()["c"]
    threshold = execute(conn, "SELECT low_stock FROM settings WHERE id=1").fetchone()
    t = threshold["low_stock"] if threshold else 5
    low_stock_count = execute(
        conn, "SELECT COUNT(*) AS c FROM products WHERE stock <= ? AND stock > 0", (t,)
    ).fetchone()["c"]
    out_of_stock_count = execute(
        conn, "SELECT COUNT(*) AS c FROM products WHERE stock = 0"
    ).fetchone()["c"]
    sale_count = execute(conn, "SELECT COUNT(*) AS c FROM sales").fetchone()["c"]
    sales_rows = rows_to_list(execute(conn, "SELECT items FROM sales").fetchall())
    conn.close()

    product_sales: dict[str, int] = {}
    for row in sales_rows:
        try:
            items = json.loads(row["items"]) if isinstance(row["items"], str) else row["items"]
            for item in items:
                key = item.get("name", "?")
                product_sales[key] = product_sales.get(key, 0) + item.get("qty", 0)
        except Exception:
            pass

    top_products = sorted(product_sales.items(), key=lambda x: x[1], reverse=True)[:5]

    return {
        "total_revenue": total_revenue,
        "total_expenses": total_expenses,
        "profit": total_revenue - total_expenses,
        "product_count": product_count,
        "low_stock_count": low_stock_count,
        "out_of_stock_count": out_of_stock_count,
        "sale_count": sale_count,
        "top_products": [{"name": n, "qty": q} for n, q in top_products],
    }


# ---------------------------------------------------------------------------
# Data import / export
# ---------------------------------------------------------------------------

@app.get("/api/export", tags=["Data"])
def export_all():
    conn = get_connection()
    products = rows_to_list(execute(conn, "SELECT * FROM products").fetchall())
    sales = [
        parse_items(r)
        for r in rows_to_list(execute(conn, "SELECT * FROM sales").fetchall())
    ]
    expenses = rows_to_list(execute(conn, "SELECT * FROM expenses").fetchall())
    settings = row_to_dict(execute(conn, "SELECT * FROM settings WHERE id=1").fetchone())
    conn.close()
    return {"products": products, "sales": sales, "expenses": expenses, "settings": settings}


class ImportData(BaseModel):
    products: list[dict] = []
    sales: list[dict] = []
    expenses: list[dict] = []
    settings: Optional[dict] = None


@app.post("/api/import", tags=["Data"])
def import_all(body: ImportData):
    conn = get_connection()
    with conn:
        if body.products:
            conn.cursor().execute("DELETE FROM products")
            for p in body.products:
                execute(
                    conn,
                    """INSERT INTO products
                       (id,code,name,cat,brand,size,ink,price,cost,stock,notes,
                        current_stock_price,avg_stock_price)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT (id) DO UPDATE SET
                         code=EXCLUDED.code, name=EXCLUDED.name, cat=EXCLUDED.cat,
                         brand=EXCLUDED.brand, size=EXCLUDED.size, ink=EXCLUDED.ink,
                         price=EXCLUDED.price, cost=EXCLUDED.cost, stock=EXCLUDED.stock,
                         notes=EXCLUDED.notes,
                         current_stock_price=EXCLUDED.current_stock_price,
                         avg_stock_price=EXCLUDED.avg_stock_price""",
                    (
                        p.get("id"), p.get("code", ""), p.get("name", ""),
                        p.get("cat", ""), p.get("brand", ""), p.get("size", ""),
                        p.get("ink", ""), p.get("price", 0), p.get("cost", 0),
                        p.get("stock", 0), p.get("notes", ""),
                        p.get("currentStockPrice", p.get("current_stock_price", 0)),
                        p.get("avgStockPrice", p.get("avg_stock_price", 0)),
                    ),
                )

        if body.sales:
            conn.cursor().execute("DELETE FROM sales")
            for s in body.sales:
                items = s.get("items", [])
                execute(
                    conn,
                    "INSERT INTO sales (id,num,date,client,payment,items,total) VALUES (?,?,?,?,?,?,?)",
                    (
                        s.get("id"), s.get("num", 0), s.get("date", ""),
                        s.get("client", ""), s.get("payment", "Espèces"),
                        json.dumps(items), s.get("total", 0),
                    ),
                )

        if body.expenses:
            conn.cursor().execute("DELETE FROM expenses")
            for e in body.expenses:
                execute(
                    conn,
                    "INSERT INTO expenses (id,date,cat,description,amount) VALUES (?,?,?,?,?)",
                    (e.get("id"), e.get("date", ""), e.get("cat", ""),
                     e.get("description", e.get("desc", "")), e.get("amount", 0)),
                )

        if body.settings:
            s = body.settings
            execute(
                conn,
                "UPDATE settings SET shop_name=?, currency=?, low_stock=?, lang=? WHERE id=1",
                (s.get("shopName", s.get("shop_name", "SIRDASProAES")),
                 s.get("currency", "CFA"),
                 s.get("lowStock", s.get("low_stock", 5)),
                 s.get("lang", "fr")),
            )

    reset_sequences(conn)
    conn.close()
    return {"message": "Import successful"}
