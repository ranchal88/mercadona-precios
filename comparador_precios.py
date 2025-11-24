import pandas as pd
import sys

def main(file_day0, file_dayX):
    print("\n===============================")
    print("  🔍 COMPARADOR DE PRECIOS")
    print("===============================")

    print(f"\n📂 Día inicial: {file_day0}")
    print(f"📂 Día actual : {file_dayX}")

    # Cargar CSVs
    df0 = pd.read_csv(file_day0)
    dfX = pd.read_csv(file_dayX)

    # Asegurar precios en euros (Mercadona ya los da en euros)
    df0["price"] = df0["unit_price"].astype(float)
    dfX["price"] = dfX["unit_price"].astype(float)

    # Seleccionar columnas relevantes
    df0_small = df0[["product_id", "name", "price"]].rename(columns={"price": "price_day0"})
    dfX_small = dfX[["product_id", "name", "price"]].rename(columns={"price": "price_dayX"})

    # Merge
    merged = df0_small.merge(dfX_small, on="product_id", how="outer", suffixes=("_day0", "_dayX"))

    # Calcular diferencias
    merged["diff"] = merged["price_dayX"] - merged["price_day0"]
    merged["pct_change"] = (merged["diff"] / merged["price_day0"]) * 100

    # Subidas y bajadas
    price_up = merged[merged["diff"] > 0].sort_values("diff", ascending=False)
    price_down = merged[merged["diff"] < 0].sort_values("diff")

    # Top 50
    top50_up = price_up.head(50)
    top50_down = price_down.head(50)

    # Variación media total (solo productos que existen en ambos días)
    comparable = merged.dropna(subset=["price_day0", "price_dayX"])
    mean_variation = comparable["pct_change"].mean()

    print("\n📈 VARIACIÓN MEDIA TOTAL DESDE EL DÍA INICIAL:")
    print(f"   {mean_variation:.2f}%")

    print("\n🔥 TOP 50 SUBIDAS DE PRECIO:")
    print(top50_up[["product_id", "name_day0", "price_day0", "price_dayX", "diff", "pct_change"]])

    print("\n❄️ TOP 50 BAJADAS DE PRECIO:")
    print(top50_down[["product_id", "name_day0", "price_day0", "price_dayX", "diff", "pct_change"]])

    print("\n🧪 Productos nuevos:")
    new_products = merged[merged["price_day0"].isna()]
    print(new_products[["product_id", "name_dayX", "price_dayX"]])

    print("\n🗑️ Productos eliminados:")
    removed_products = merged[merged["price_dayX"].isna()]
    print(removed_products[["product_id", "name_day0", "price_day0"]])

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso:")
        print("   python comparador_precios.py archivo_dia_20 archivo_dia_actual")
        sys.exit(1)

    file_day0 = sys.argv[1]
    file_dayX  = sys.argv[2]

    main(file_day0, file_dayX)
