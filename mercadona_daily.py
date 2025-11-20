import os
import requests
import pandas as pd
from datetime import date

# Parámetros de la tienda
WAREHOUSE = "mad1"
LANG = "es"

# Rango razonable de IDs de categoría a probar
MAX_ID = 300

BASE_URL = "https://tienda.mercadona.es/api/categories/{}/?lang={}&wh={}"


def discover_categories(warehouse=WAREHOUSE, lang=LANG, max_id=MAX_ID):
    """
    Descubre automáticamente las categorías válidas (status 200).
    Esto permite que, si Mercadona añade nuevas categorías,
    se detecten solas sin tocar el código.
    """
    categorias_validas = []

    for cat_id in range(1, max_id + 1):
        url = BASE_URL.format(cat_id, lang, warehouse)
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                categorias_validas.append(cat_id)
        except Exception as e:
            # Simplemente lo logueamos y seguimos
            print(f"⚠️ Error al probar categoría {cat_id}: {e}")

    print(f"✔ Categorías válidas encontradas: {len(categorias_validas)}")
    return categorias_validas


def get_category_products(cat_id, warehouse=WAREHOUSE, lang=LANG):
    """
    Descarga todos los productos de una categoría concreta.
    """
    url = BASE_URL.format(cat_id, lang, warehouse)
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"⚠️ Error al descargar categoría {cat_id}: {e}")
        return []

    productos = []

    for subcat in data.get("categories", []):
        subcat_id = subcat.get("id")
        subcat_name = subcat.get("name")

        for p in subcat.get("products", []):
            precio = p.get("price_instructions", {})

            productos.append({
                "category_id": cat_id,
                "subcategory_id": subcat_id,
                "subcategory_name": subcat_name,
                "product_id": p.get("id"),
                "name": p.get("display_name"),
                "slug": p.get("slug"),
                "thumbnail": p.get("thumbnail"),
                "share_url": p.get("share_url"),
                "packaging": p.get("packaging"),
                "published": p.get("published"),
                "unit_price": precio.get("unit_price"),
                "bulk_price": precio.get("bulk_price"),
                "unit_size": precio.get("unit_size"),
                "size_format": precio.get("size_format"),
                "selling_method": precio.get("selling_method"),
                "is_new": precio.get("is_new"),
                "price_decreased": precio.get("price_decreased"),
            })

    return productos


def main():
    # 1) Descubrir categorías dinámicamente
    categorias = discover_categories()

    all_products = []

    print("📦 Descargando productos de todas las categorías...\n")

    for cat_id in categorias:
        productos = get_category_products(cat_id)
        all_products.extend(productos)

    if not all_products:
        print("❌ No se han obtenido productos. Revisa el script / la conexión.")
        return

    df = pd.DataFrame(all_products)

    # 2) Añadir columna de fecha (para histórico)
    today = date.today().isoformat()  # YYYY-MM-DD
    df["date"] = today

    # 3) Asegurar carpeta data/
    os.makedirs("data", exist_ok=True)

    # 4) Guardar CSV diario
    output_path = os.path.join("data", f"mercadona_{today}.csv")
    df.to_csv(output_path, index=False, encoding="utf-8")

    print(f"🛒 Total productos recopilados: {len(df)}")
    print(f"💾 Archivo guardado en: {output_path}")


if __name__ == "__main__":
    main()
