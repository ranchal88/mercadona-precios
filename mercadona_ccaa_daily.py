import os
import requests
import pandas as pd
from datetime import date

# Parámetros generales
LANG = "es"
MAX_CATEGORY_ID = 300  # rango de IDs de categorías a probar

BASE_URL = "https://tienda.mercadona.es/api/categories/{}/?lang={}&wh={}"

# ==========
# MAPEOS CCAA → LISTA DE WAREHOUSES
# (a partir de lo que has sacado tú)
# ==========

CCAA_WAREHOUSES = {
    "andalucia": [
        "4354",   # Almería
        "svq1",   # Cádiz, Málaga, Huelva, Sevilla
        "4694",   # Córdoba
        "3968",   # Granada
        "4544",   # Jaén
    ],
    "aragon": [
        "4389",   # Huesca
        "4665",   # Teruel
        "4493",   # Zaragoza
    ],
    "asturias": [
        "4480",   # Oviedo
    ],
    "islas_baleares": [
        "3842",   # Palma, Ibiza, Menorca
    ],
    "canarias": [
        "4701",   # Las Palmas
        "3280",   # Tenerife
    ],
    "cantabria": [
        "4522",   # Santander
    ],
    "castilla_y_leon": [
        "4471",   # Ávila
        "4346",   # Burgos
        "3683",   # León
        "3880",   # Palencia
        "3681",   # Salamanca
        "4077",   # Segovia
        "2316",   # Soria
        "4735",   # Valladolid
        "4673",   # Zamora
    ],
    "castilla_la_mancha": [
        "4587",   # Albacete
        "4568",   # Ciudad Real / Toledo
        "4606",   # Cuenca
        "4241",   # Guadalajara
    ],
    "cataluna": [
        "bcn1",   # Barcelona
        "2004",   # Girona
        "4115",   # Lleida
        "3947",   # Tarragona
    ],
    "comunidad_valenciana": [
        "alc1",   # Alicante
        "4558",   # Castellón
        "vlc1",   # Valencia
    ],
    "extremadura": [
        "3497",   # Badajoz
        "4055",   # Cáceres
    ],
    "galicia": [
        "4166",   # A Coruña
        "4450",   # Lugo
        "4592",   # Ourense
        "4655",   # Pontevedra
    ],
    "madrid": [
        "mad1",   # Madrid
    ],
    "murcia": [
        "alc1",   # Murcia comparte almacén con Alicante
    ],
    "navarra": [
        "4229",   # Pamplona
    ],
    "la_rioja": [
        "4375",   # Logroño
    ],
    "pais_vasco": [
        "4697",   # Álava
        "4331",   # Gipuzkoa
        "4391",   # Bizkaia
    ],
}


def discover_categories(warehouse: str, lang: str = LANG, max_id: int = MAX_CATEGORY_ID):
    """
    Descubre automáticamente las categorías válidas para un warehouse concreto.
    (Mismo enfoque que tu script original, pero parametrizado por warehouse.)
    """
    categorias_validas = []

    for cat_id in range(1, max_id + 1):
        url = BASE_URL.format(cat_id, lang, warehouse)
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                categorias_validas.append(cat_id)
        except Exception as e:
            print(f"⚠️ Error al probar categoría {cat_id} para wh={warehouse}: {e}")

    print(f"✔ [wh={warehouse}] Categorías válidas encontradas: {len(categorias_validas)}")
    return categorias_validas


def get_category_products(cat_id: int, warehouse: str, lang: str = LANG):
    """
    Descarga todos los productos de una categoría concreta y un warehouse concreto.
    """
    url = BASE_URL.format(cat_id, lang, warehouse)
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"⚠️ Error al descargar categoría {cat_id} para wh={warehouse}: {e}")
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
                # Mercadona ya devuelve precios en euros
                "unit_price": precio.get("unit_price"),
                "bulk_price": precio.get("bulk_price"),
                "unit_size": precio.get("unit_size"),
                "size_format": precio.get("size_format"),
                "selling_method": precio.get("selling_method"),
                "is_new": precio.get("is_new"),
                "price_decreased": precio.get("price_decreased"),
                "warehouse": warehouse,
            })

    return productos


def scrape_ccaa(ccaa_key: str, warehouses: list, today: str):
    """
    Scrapea todos los warehouses de una CCAA, junta sus productos
    y guarda un CSV por comunidad autónoma y día.
    """
    print(f"\n🏴‍☠️ Scrapeando CCAA: {ccaa_key}")

    all_products = []

    # Usamos set(...) por si algún warehouse se repite
    for wh in set(warehouses):
        print(f"  📦 Warehouse: {wh}")
        categorias = discover_categories(warehouse=wh)

        for cat_id in categorias:
            productos = get_category_products(cat_id, warehouse=wh)
            all_products.extend(productos)

    if not all_products:
        print(f"❌ No se han obtenido productos para {ccaa_key}.")
        return

    df = pd.DataFrame(all_products)
    df["ccaa"] = ccaa_key
    df["date"] = today

    # Carpeta por CCAA: data/ccaa_key/
    region_folder = os.path.join("data", ccaa_key)
    os.makedirs(region_folder, exist_ok=True)

    output_path = os.path.join(region_folder, f"mercadona_{ccaa_key}_{today}.csv")
    df.to_csv(output_path, index=False, encoding="utf-8")

    print(f"🛒 [{ccaa_key}] Total productos recopilados: {len(df)}")
    print(f"💾 Archivo guardado en: {output_path}")


def main():
    today = date.today().isoformat()
    print(f"📅 Ejecutando scrape diario por CCAA. Fecha: {today}")

    for ccaa_key, wh_list in CCAA_WAREHOUSES.items():
        scrape_ccaa(ccaa_key, wh_list, today)

    print("\n✅ Scrape completo de todas las CCAA.")


if __name__ == "__main__":
    main()
